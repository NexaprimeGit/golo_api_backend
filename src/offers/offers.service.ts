import { BadRequestException, Injectable, Logger, NotFoundException, OnModuleInit, Optional } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, isValidObjectId } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { KafkaService } from '../kafka/kafka.service';
import { RedisService } from '../common/services/redis.service';
import { OfferPromotion, OfferPromotionDocument, OfferPromotionStatus, OfferPaymentStatus } from './schemas/offer-promotion.schema';
// Banner models removed to keep offers module independent from banners
import { User, UserDocument } from '../users/schemas/user.schema';
import { Merchant, MerchantDocument } from '../users/schemas/merchant.schema';
import { KAFKA_TOPICS } from '../common/constants/kafka-topics';

// Bug Fix #4: Default placeholder image for offers without imageUrl
const DEFAULT_OFFER_IMAGE = '/images/deal1.jpg';

@Injectable()
export class OffersService implements OnModuleInit {
  private readonly logger = new Logger(OffersService.name);
  private readonly cacheTtlSeconds = {
    merchantOffers: 30,      // Short TTL so Device B sees new offers within 30s
    publicOffer: 300,
    template: 600,
    nearbyOffers: 120,       // 2 min — nearby is public data, safe to cache longer
  } as const;

  constructor(
    @InjectModel(OfferPromotion.name) private readonly offerModel: Model<OfferPromotionDocument>,
    @InjectModel(User.name) private readonly userModel: Model<UserDocument>,
    @InjectModel(Merchant.name) private readonly merchantModel: Model<MerchantDocument>,
    private readonly redisService: RedisService,
    @Optional() private readonly kafkaService?: KafkaService,
  ) {}

  private offerMerchantCacheKey(merchantId: string) {
    return `golo:offers:merchant:${merchantId}`;
  }

  private offerTemplateCacheKey(merchantId: string) {
    return `golo:offers:template:${merchantId}`;
  }

  private publicOfferCacheKey(identifier: string) {
    return `golo:offers:public:${identifier}`;
  }

  private nearbyOffersCacheKey(params: any): string {
    const hasCoordinates = Number.isFinite(Number(params?.latitude)) && Number.isFinite(Number(params?.longitude));

    const normalizedKey = {
      activeNow: params?.activeNow === undefined ? true : Boolean(params.activeNow),
      category: String(params?.category || '').trim().toLowerCase(),
      lat: hasCoordinates ? Number(Number(params.latitude).toFixed(2)) : null,
      lng: hasCoordinates ? Number(Number(params.longitude).toFixed(2)) : null,
      limit: Math.min(50, Math.max(1, Number(params?.limit) || 20)),
      location: String(params?.location || '').trim().toLowerCase(),
      maxPrice: Number.isFinite(Number(params?.maxPrice)) && Number(params.maxPrice) > 0 ? Number(params.maxPrice) : null,
      offerTypes: String(params?.offerTypes || '').trim().toLowerCase(),
      page: Math.max(1, Number(params?.page) || 1),
      query: String(params?.query || '').trim().toLowerCase(),
      radiusKm: Math.min(100, Math.max(1, Number(params?.radiusKm) || 5)),
      sort: String(params?.sort || '').trim().toLowerCase(),
      topDiscount: Boolean(params?.topDiscount),
    };

    // Hash the params object to keep Redis key short (< 200 chars).
    const raw = JSON.stringify(normalizedKey);
    let hash = 0;
    for (let i = 0; i < raw.length; i++) {
      const chr = raw.charCodeAt(i);
      hash = (hash << 5) - hash + chr;
      hash |= 0; // Convert to 32-bit int
    }
    return `golo:offers:nearby:${Math.abs(hash).toString(36)}`;
  }

  private async readCache<T>(key: string): Promise<T | null> {
    return this.redisService.get<T>(key);
  }

  private async writeCache(key: string, value: any, ttlSeconds: number) {
    await this.redisService.set(key, value, ttlSeconds);
  }

  // Use del() for exact keys and SCAN-based deleteByPattern only for wildcards.
  private async clearCache(keyOrPattern: string): Promise<void> {
    await this.redisService.deleteByPattern(keyOrPattern);
  }

  private async clearExactKey(key: string): Promise<void> {
    await this.redisService.del(key);
  }

  private async clearOfferCaches(merchantId: string, identifiers: string[] = []): Promise<void> {
    await Promise.all([
      // Exact merchant key — use del, not pattern scan
      this.clearExactKey(this.offerMerchantCacheKey(merchantId)),
      // Template is also an exact key
      this.clearExactKey(this.offerTemplateCacheKey(merchantId)),
      // Nearby is a wildcard pattern — use SCAN-based deleteByPattern
      this.clearCache('golo:offers:nearby:*'),
      // Public offer keys by known identifiers (exact)
      ...identifiers.map((id) => this.clearExactKey(this.publicOfferCacheKey(id))),
    ]);
  }

  private readonly legacyOfferCategorySet = new Set([
    'special',
    'festival',
    'limited time',
    'combo',
    'clearance',
  ]);

  private toRadians(value: number): number {
    return (value * Math.PI) / 180;
  }

  private calculateDistanceKm(
    latitudeA: number,
    longitudeA: number,
    latitudeB: number,
    longitudeB: number,
  ): number {
    const earthRadiusKm = 6371;
    const dLat = this.toRadians(latitudeB - latitudeA);
    const dLon = this.toRadians(longitudeB - longitudeA);

    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(this.toRadians(latitudeA)) *
        Math.cos(this.toRadians(latitudeB)) *
        Math.sin(dLon / 2) *
        Math.sin(dLon / 2);

    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return earthRadiusKm * c;
  }

  private hasValidMerchantCoordinates(latitude: number, longitude: number): boolean {
    const inValidRange =
      Number.isFinite(latitude) &&
      Number.isFinite(longitude) &&
      latitude >= -90 &&
      latitude <= 90 &&
      longitude >= -180 &&
      longitude <= 180;

    if (!inValidRange) return false;
    return !(Math.abs(latitude) < 0.000001 && Math.abs(longitude) < 0.000001);
  }

  private normalizeVisibilityDates(input: any[] = []): Date[] {
    const dates = Array.isArray(input) ? input : [];
    const normalized = dates
      .map((value: any) => new Date(value))
      .filter((d: Date) => !Number.isNaN(d.getTime()))
      .map((d: Date) => {
        const day = new Date(d);
        // Use UTC day boundaries so the server timezone doesn't shift visibility windows.
        day.setUTCHours(0, 0, 0, 0);
        return day;
      })
      .sort((a: Date, b: Date) => a.getTime() - b.getTime());

    // De-dup by day timestamp after normalization.
    return Array.from(new Map(normalized.map((d) => [d.getTime(), d])).values());
  }

  private buildVisibilityRange(selectedDates: Date[]) {
    const sorted = selectedDates.slice().sort((a, b) => a.getTime() - b.getTime());
    const startDate = new Date(sorted[0]);
    startDate.setUTCHours(0, 0, 0, 0);
    const endDate = new Date(sorted[sorted.length - 1]);
    // Inclusive end-of-day so offers remain visible throughout the end date.
    endDate.setUTCHours(23, 59, 59, 999);
    return { selectedDates: sorted, startDate, endDate };
  }

  private computeOfferPricing(row: any) {
    const selectedProducts: any[] = Array.isArray(row?.selectedProducts) ? row.selectedProducts : [];

    const computedBestDiscountPercent = selectedProducts.reduce((best, product) => {
      const original = Number(product?.originalPrice || 0);
      const offerPrice = Number(product?.offerPrice || 0);
      if (original <= 0 || offerPrice < 0 || offerPrice >= original) return best;
      const discountPercent = ((original - offerPrice) / original) * 100;
      return Math.max(best, discountPercent);
    }, 0);

    const summedOfferPrice = selectedProducts.reduce((sum, product) => {
      const value = Number(product?.offerPrice || 0);
      return value > 0 ? sum + value : sum;
    }, 0);

    // Calculate actual aggregate discount:
    const totalOriginal = selectedProducts.reduce((sum, p) => sum + Number(p.originalPrice || 0), 0);
    const totalOffer = summedOfferPrice;
    const correctDiscountPercent = totalOriginal > 0 
      ? Math.round(((totalOriginal - totalOffer) / totalOriginal) * 100)
      : 0;

    return {
      selectedProducts,
      displayPrice: summedOfferPrice > 0 ? summedOfferPrice : Number(row?.totalPrice || 0),
      discountPercent: correctDiscountPercent,
    };
  }

  private normalizeMerchantOfferRow(row: any) {
    const pricing = this.computeOfferPricing(row);
    const startsAt = row?.startDate || row?.selectedDates?.[0] || null;
    const endsAt = row?.endDate || (Array.isArray(row?.selectedDates) ? row.selectedDates[row.selectedDates.length - 1] : null) || null;

    // Bug Fix #5: Ensure consistent ID handling
    // Note: API returns both:
    //   - offerId: MongoDB ObjectId as string (from _id)
    //   - requestId: Unique identifier assigned at offer creation (more stable)
    // Clients should prefer requestId for navigation when available
    return {
      ...row,
      title: row?.title || '',
      category: row?.category || '',
      // Bug Fix #4: Use default image if missing
      imageUrl: row?.imageUrl || DEFAULT_OFFER_IMAGE,
      totalPrice: Number(row?.totalPrice || 0),
      displayPrice: pricing.displayPrice,
      discountPercent: pricing.discountPercent,
      startsAt,
      endsAt,
      selectedProducts: pricing.selectedProducts,
      selectedDates: Array.isArray(row?.selectedDates) ? row.selectedDates : [],
    };
  }

  // Legacy detection removed — offers are handled only from `offers` collection
  private isLikelyLegacyOffer(_: any): boolean {
    return false;
  }

  async onModuleInit() {
    try {
      const indexes = await this.offerModel.collection.indexes();
      const staleIdempotencyIndexes = indexes.filter((idx: any) => {
        const keys = Object.keys(idx?.key || {});
        return keys.includes('idempotencyKey');
      });

      for (const indexDef of staleIdempotencyIndexes) {
        const indexName = indexDef?.name;
        if (!indexName) continue;
        await this.offerModel.collection.dropIndex(indexName);
        this.logger.warn(`[Offers] Dropped legacy index on startup: ${indexName}`);
      }
    } catch (error: any) {
      this.logger.warn(`[Offers] Index cleanup skipped: ${error?.message || 'unknown error'}`);
    }
    // Start background status synchronization to flip offers from under_review -> active and active -> expired
    try {
      // Run once at startup
      this.syncOfferStatuses().catch((err) => this.logger.warn(`[Offers] Initial sync failed: ${err?.message || err}`));
      // Schedule periodic sync every 60 seconds
      setInterval(() => {
        this.syncOfferStatuses().catch((err) => this.logger.warn(`[Offers] Periodic sync failed: ${err?.message || err}`));
      }, 60 * 1000);
      this.logger.log('[Offers] Scheduled background status sync every 60s');
    } catch (err) {
      this.logger.warn('[Offers] Scheduling status sync skipped: ' + String(err));
    }
  }

  async submitOfferPromotionRequest(merchantId: string, payload: any) {
    try {
      if (!payload || typeof payload !== 'object') {
        throw new BadRequestException('Invalid offer payload');
      }

      this.logger.log(`[submitOfferPromotionRequest] merchantId=${merchantId}, payload keys=${Object.keys(payload || {}).join(',')}`);
      
      const merchant = await this.userModel.findById(merchantId).select('name email role accountType').lean().exec();
      if (!merchant) {
        this.logger.error(`[submitOfferPromotionRequest] Merchant not found: ${merchantId}`);
        throw new NotFoundException('Merchant not found');
      }
      this.logger.log(`[submitOfferPromotionRequest] Merchant found: ${merchant.name}, role=${merchant.role}, accountType=${merchant.accountType}`);
      
      if (merchant.role !== 'merchant' && merchant.accountType !== 'merchant') {
        throw new BadRequestException('Only merchants can submit offers');
      }

       const merchantProfile = await this.merchantModel.findOne({ userId: merchantId })
         .select('storeCategory storeSubCategory storeLocationLatitude storeLocationLongitude')
         .lean()
         .exec();
       this.logger.log(`[submitOfferPromotionRequest] Merchant profile: ${JSON.stringify(merchantProfile)}`);

       const latitude = Number(merchantProfile?.storeLocationLatitude);
       const longitude = Number(merchantProfile?.storeLocationLongitude);
       const hasCoords = Number.isFinite(latitude) && Number.isFinite(longitude);
       if (!hasCoords) throw new BadRequestException('Store coordinates missing. Set store location before publishing offers.');

       // Determine business category from merchant profile (fallbacks to payload or generic)
       const businessCategory = (merchantProfile?.storeCategory || '').trim() || (payload.category || '').trim() || 'General';
       const businessSubCategory = (merchantProfile?.storeSubCategory || '').trim();

    const normalizedDates = this.normalizeVisibilityDates(payload.selectedDates);
    if (!normalizedDates.length) throw new BadRequestException('Please select at least one valid visibility date');

    const todayUtc = new Date();
    todayUtc.setUTCHours(0, 0, 0, 0);
    if (normalizedDates[0] < todayUtc) throw new BadRequestException('Selected dates cannot be in the past');

    const { selectedDates, startDate, endDate } = this.buildVisibilityRange(normalizedDates);

    const selectedDays = normalizedDates.length;
    const dailyRate = Number(payload.dailyRate ?? 240);
    const platformFee = Number(payload.platformFee ?? (selectedDays > 0 ? 49 : 0));
    const computedTotal = dailyRate * selectedDays + platformFee;

    // Determine initial status based on startDate (UTC day boundaries)
    const nowUtc = new Date();
    nowUtc.setUTCHours(0, 0, 0, 0);
    const initialStatus = startDate <= nowUtc ? OfferPromotionStatus.ACTIVE : OfferPromotionStatus.UNDER_REVIEW;

     const request = await this.offerModel.create({
       requestId: uuidv4(),
       merchantId,
       merchantName: merchant.name || 'Merchant',
       merchantEmail: merchant.email || '-',
       title: (payload.title || '').trim(),
       // Preserve promotional category from merchant (used for UI filtering)
       category: (payload.category || '').trim(),
       // Business category for matching user preferences
       businessCategory,
       businessSubCategory,
       // Optional promotion tag for extra filtering
       promotionType: payload.promoTag || '',
       description: payload.description || '',
       imageUrl: payload.imageUrl,
       recommendedSize: payload.recommendedSize || '1920 x 520 px',
       selectedDates,
       startDate,
       endDate,
       selectedDays,
       dailyRate,
       platformFee,
       totalPrice: Number(payload.totalPrice || computedTotal),
       loyaltyRewardEnabled: Boolean(payload.loyaltyRewardEnabled),
       loyaltyStarsToOffer: Number(payload.loyaltyStarsToOffer || 0),
       loyaltyStarsPerPurchase: Number(payload.loyaltyStarsPerPurchase || 1),
       loyaltyScorePerStar: Number(payload.loyaltyScorePerStar || 10),
         loyaltyPointsPerPurchase: Number(payload.loyaltyPointsPerPurchase || 0),
       promotionExpiryText: payload.promotionExpiryText || '',
       termsAndConditions: payload.termsAndConditions || '',
       exampleUsage: payload.exampleUsage || '',
       selectedProducts: Array.isArray(payload.selectedProducts) ? payload.selectedProducts : [],
      status: initialStatus,
       paymentStatus: OfferPaymentStatus.PENDING,
       isActive: initialStatus === OfferPromotionStatus.ACTIVE,
     });

    if (this.kafkaService) {
      try {
        await this.kafkaService.emit(KAFKA_TOPICS.OFFER_PROMOTION_SUBMITTED, {
          requestId: request.requestId,
          merchantId,
          title: request.title,
          category: request.category,
          totalPrice: request.totalPrice,
        });
      } catch (err) {
        this.logger.warn('Kafka emit failed for offer submission');
      }
    }

     await this.clearOfferCaches(merchantId, [request.requestId, String(request._id)]);
     
     // Clear recommendation cache for users who might match this offer's business category
     if (businessCategory) {
       try {
         const pattern = `reco:deals:v2:user:*`;
         await this.redisService.deleteByPattern(pattern);
         this.logger.log(`[Offers] Cleared recommendation cache for new offer (category: ${businessCategory})`);
       } catch (err) {
         this.logger.warn('Failed to clear recommendation cache:', err.message);
       }
     }
     
     return request;
  } catch (error: any) {
    if (error?.name === 'ValidationError' || error?.name === 'CastError') {
      throw new BadRequestException(error?.message || 'Invalid offer payload');
    }

    if (error?.code === 11000) {
      this.logger.warn(`[submitOfferPromotionRequest] E11000 Duplicate Key Error: ${error.message}`);
      throw new BadRequestException('Failed to create offer. Please try again.');
    }

    this.logger.error(`[submitOfferPromotionRequest] Error: ${error.message}`, error.stack);
    throw error;
  }
  }

  async listMerchantOffers(merchantId: string) {
    const cacheKey = this.offerMerchantCacheKey(merchantId);
    const cached = await this.readCache<any[]>(cacheKey);
    if (cached) {
      return cached;
    }

    // Support both merchant userId and legacy merchant profile _id stored in some rows
    const merchantProfile = await this.merchantModel.findOne({ userId: merchantId }).select('_id').lean().exec();
    const profileId = merchantProfile?._id ? String(merchantProfile._id) : null;

    const query: any = profileId
      ? { $or: [{ merchantId: merchantId }, { merchantId: profileId }] }
      : { merchantId: merchantId };

    const rows = await this.offerModel.find(query).sort({ createdAt: -1 }).lean().exec();
    const normalized = rows.map((row) => this.normalizeMerchantOfferRow(row));
    await this.writeCache(cacheKey, normalized, this.cacheTtlSeconds.merchantOffers);
    return normalized;
  }

  async updateMerchantOffer(requestId: string, merchantId: string, payload: any) {
    const request = await this.offerModel.findOne({ requestId, merchantId }).exec();
    if (!request) throw new NotFoundException('Offer not found');

    const nextTitle = payload.title ?? payload.bannerTitle;
    const nextCategory = payload.category ?? payload.bannerCategory;
    if (nextTitle !== undefined) request.title = String(nextTitle || '').trim();
    if (nextCategory !== undefined) request.category = String(nextCategory || '').trim();
    if (payload.imageUrl !== undefined) request.imageUrl = String(payload.imageUrl || '').trim();
    if (payload.description !== undefined) request.description = String(payload.description || '').trim();
    if (payload.recommendedSize !== undefined) request.recommendedSize = payload.recommendedSize;
    if (payload.loyaltyRewardEnabled !== undefined) request.loyaltyRewardEnabled = Boolean(payload.loyaltyRewardEnabled);
    if (payload.loyaltyStarsToOffer !== undefined) request.loyaltyStarsToOffer = Number(payload.loyaltyStarsToOffer || 0);
    if (payload.loyaltyStarsPerPurchase !== undefined) request.loyaltyStarsPerPurchase = Number(payload.loyaltyStarsPerPurchase || 0);
    if (payload.loyaltyScorePerStar !== undefined) request.loyaltyScorePerStar = Number(payload.loyaltyScorePerStar || 0);
    if (payload.loyaltyPointsPerPurchase !== undefined) request.loyaltyPointsPerPurchase = Number(payload.loyaltyPointsPerPurchase || 0);
    if (payload.promotionExpiryText !== undefined) request.promotionExpiryText = String(payload.promotionExpiryText || '').trim();
    if (payload.termsAndConditions !== undefined) request.termsAndConditions = String(payload.termsAndConditions || '').trim();
    if (payload.exampleUsage !== undefined) request.exampleUsage = String(payload.exampleUsage || '').trim();
    if (payload.dailyRate !== undefined) request.dailyRate = Number(payload.dailyRate || 0);
    if (payload.platformFee !== undefined) request.platformFee = Number(payload.platformFee || 0);
    if (payload.totalPrice !== undefined) request.totalPrice = Number(payload.totalPrice || 0);
    if (Array.isArray(payload.selectedDates) && payload.selectedDates.length) {
      const normalized = this.normalizeVisibilityDates(payload.selectedDates);
      if (!normalized.length) throw new BadRequestException('Please provide valid selectedDates');
      const { selectedDates, startDate, endDate } = this.buildVisibilityRange(normalized);
      request.selectedDates = selectedDates;
      request.startDate = startDate;
      request.endDate = endDate;
      request.selectedDays = selectedDates.length;
      request.totalPrice = request.dailyRate * request.selectedDays + request.platformFee;
    }

    if (Array.isArray(payload.selectedProducts)) {
      request.selectedProducts = payload.selectedProducts;
      const pricing = this.computeOfferPricing(request);
      request.totalPrice = Number(payload.totalPrice || pricing.displayPrice || request.totalPrice || 0);
    }

    request.markModified('selectedProducts');
    request.markModified('selectedDates');
    request.markModified('title');
    request.markModified('category');
    request.markModified('imageUrl');

    if (payload.action === 'pause') {
      request.isActive = false;
      if (request.status === OfferPromotionStatus.ACTIVE) request.status = OfferPromotionStatus.APPROVED;
    }

    if (payload.action === 'resume') {
      if (request.paymentStatus !== OfferPaymentStatus.PAID) throw new BadRequestException('Only paid offers can be resumed');
      request.isActive = true;
      request.status = OfferPromotionStatus.ACTIVE;
    }

    await request.save();
    await this.clearOfferCaches(merchantId, [requestId, String(request._id)]);
    return this.normalizeMerchantOfferRow(request.toObject());
  }

  async deleteMerchantOffer(requestId: string, merchantId: string) {
    const offer = await this.offerModel.findOne({ requestId, merchantId }).exec();
    if (!offer) throw new NotFoundException('Offer not found');
    await this.offerModel.deleteOne({ requestId, merchantId }).exec();
    if (this.kafkaService) {
      try { await this.kafkaService.emit(KAFKA_TOPICS.OFFER_PROMOTION_DELETED, { requestId, merchantId }); } catch {}
    }
    await this.clearOfferCaches(merchantId, [requestId, String(offer._id)]);
    return offer;
  }

  async getPublicOfferDetails(offerId: string) {
    const cacheKey = this.publicOfferCacheKey(offerId);
    const cached = await this.readCache<any>(cacheKey);
    if (cached) {
      return cached;
    }

    let row: any = null;

    // Try MongoDB _id first
    if (isValidObjectId(offerId)) {
      row = await this.offerModel.findOne({ _id: offerId }).lean().exec();
    }

    // Fallback: try requestId
    if (!row) {
      row = await this.offerModel.findOne({ requestId: offerId }).lean().exec();
    }

    if (!row) throw new NotFoundException('Offer not found');

    const merchant = await this.merchantModel
      .findOne({ userId: String(row.merchantId) })
      .select('userId storeName storeLocation storeLocationLatitude storeLocationLongitude profilePhoto shopPhoto storeCategory storeSubCategory')
      .lean()
      .exec();

    const rowAny: any = row;
    const selectedProducts: any[] = Array.isArray(rowAny.selectedProducts) ? rowAny.selectedProducts : [];

    const computedBestDiscountPercent = selectedProducts.reduce((best, product) => {
      const original = Number(product?.originalPrice || 0);
      const offerPrice = Number(product?.offerPrice || 0);
      if (original <= 0 || offerPrice < 0 || offerPrice >= original) return best;
      const discountPercent = ((original - offerPrice) / original) * 100;
      return Math.max(best, discountPercent);
    }, 0);

    const lowestOfferPrice = selectedProducts.length
      ? selectedProducts.reduce((min, product) => {
          const value = Number(product?.offerPrice || 0);
          return value > 0 ? Math.min(min, value) : min;
        }, Number.MAX_SAFE_INTEGER)
      : Number.MAX_SAFE_INTEGER;

    const startsAt = row.startDate || row.selectedDates?.[0] || null;
    const endsAt = row.endDate || (Array.isArray(row.selectedDates) ? row.selectedDates[row.selectedDates.length - 1] : null) || null;

    const normalized = {
      offerId: String(row._id),
      requestId: row.requestId,
      title: rowAny.title || '',
      category: rowAny.category || '',
      imageUrl: rowAny.imageUrl || '',
      totalPrice: Number(row.totalPrice || 0),
      displayPrice: this.computeOfferPricing(row).displayPrice,
      discountPercent: this.computeOfferPricing(row).discountPercent,
      startsAt,
      endsAt,
      status: row.status,
      isActiveNow: Boolean(startsAt && endsAt) && new Date(startsAt) <= new Date() && new Date(endsAt) >= new Date(),
      merchant: {
        merchantId: String(row.merchantId),
        name: merchant?.storeName || row.merchantName || 'Merchant',
        category: merchant?.storeCategory || '',
        subCategory: merchant?.storeSubCategory || '',
        address: merchant?.storeLocation || '',
        latitude: merchant?.storeLocationLatitude || null,
        longitude: merchant?.storeLocationLongitude || null,
        profilePhoto: merchant?.profilePhoto || merchant?.shopPhoto || '',
      },
      selectedProducts,
      createdAt: rowAny.createdAt,
      description: rowAny.description || rowAny.promotionExpiryText || '',
      exampleUsage: rowAny.exampleUsage || '',
      termsAndConditions: rowAny.termsAndConditions || '',
    };

    await Promise.all([
      this.writeCache(cacheKey, normalized, this.cacheTtlSeconds.publicOffer),
      this.writeCache(this.publicOfferCacheKey(String(row._id)), normalized, this.cacheTtlSeconds.publicOffer),
      row.requestId ? this.writeCache(this.publicOfferCacheKey(String(row.requestId)), normalized, this.cacheTtlSeconds.publicOffer) : Promise.resolve(),
    ]);

    return normalized;
  }

  // Template helpers using Redis
  async saveOfferTemplate(merchantId: string, payload: any) {
    const normalized = { formData: payload.formData || {}, selectedProducts: Array.isArray(payload.selectedProducts) ? payload.selectedProducts : [], updatedAt: new Date().toISOString() };
    const updated = await this.merchantModel.findOneAndUpdate(
      { userId: merchantId },
      { $set: { offerTemplate: normalized, updatedAt: new Date() } },
      { new: true },
    ).lean().exec();
    if (!updated) throw new BadRequestException('Failed to save template');
    await this.clearCache(this.offerTemplateCacheKey(merchantId));
    return normalized;
  }

  async getOfferTemplate(merchantId: string) {
    const cacheKey = this.offerTemplateCacheKey(merchantId);
    const cached = await this.readCache<any>(cacheKey);
    if (cached !== null) {
      return cached;
    }

    const merchant = await this.merchantModel.findOne({ userId: merchantId }).select('offerTemplate').lean().exec();
    const template = merchant?.offerTemplate || null;
    await this.writeCache(cacheKey, template, this.cacheTtlSeconds.template);
    return template;
  }

  async clearOfferTemplate(merchantId: string) {
    const updated = await this.merchantModel.findOneAndUpdate({ userId: merchantId }, { $set: { offerTemplate: null, updatedAt: new Date() } }, { new: true }).lean().exec();
    if (!updated) throw new BadRequestException('Failed to clear template');
    await this.clearCache(this.offerTemplateCacheKey(merchantId));
    return { cleared: true };
  }

  async getNearbyOffers(params: {
    latitude?: number;
    longitude?: number;
    radiusKm?: number;
    location?: string;
    query?: string;
    category?: string;
    sort?: string;
    maxPrice?: number;
    offerTypes?: string | undefined;
    topDiscount?: boolean | undefined;
    activeNow?: boolean | undefined;
    page?: number;
    limit?: number;
  }) {
    const safePage = Math.max(1, Number(params.page) || 1);
    const safeLimit = Math.min(50, Math.max(1, Number(params.limit) || 20));
    const prefetchLimit = Math.min(300, Math.max(120, safeLimit * 8));
    const safeRadiusKm = Math.min(100, Math.max(1, Number(params.radiusKm) || 5));
    const locationNeedle = String(params.location || '').trim().toLowerCase();
    const locationTokens = locationNeedle
      .split(/[\s,]+/)
      .map((token) => token.trim())
      .filter((token) => token.length >= 3);
    const queryNeedle = String(params.query || '').trim().toLowerCase();
    const categoryNeedle = String(params.category || '').trim().toLowerCase();
    const sortBy = String(params.sort || '').trim().toLowerCase();
    const maxPrice = Number(params.maxPrice);
    const offerTypesRaw = String(params.offerTypes || '').trim();
    const offerTypeTokens = offerTypesRaw
      ? offerTypesRaw
          .split(/[,\|;]+/)
          .map((t) => String(t || '').trim().toLowerCase())
          .filter(Boolean)
      : [];
    const topDiscountOnly = Boolean(params.topDiscount);
    const activeNowOnly = params.activeNow === undefined ? true : Boolean(params.activeNow);
    const cacheKey = this.nearbyOffersCacheKey(params);
    const cached = await this.readCache<{ data: any[]; pagination: { page: number; limit: number; total: number; pages: number } }>(cacheKey);
    if (cached) {
      return cached;
    }

    const normalizeCategoryToken = (value: any) =>
      String(value || '')
        .toLowerCase()
        .replace(/&/g, ' and ')
        .replace(/[^a-z0-9\s]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

    const normalizedCategoryNeedle = normalizeCategoryToken(categoryNeedle);

    const hasUserCoordinates =
      typeof params.latitude === 'number' &&
      !Number.isNaN(params.latitude) &&
      typeof params.longitude === 'number' &&
      !Number.isNaN(params.longitude);

    const locationMatchesAddress = (addressValue: string) => {
      const address = String(addressValue || '').toLowerCase();
      if (!locationNeedle) return true;
      if (address.includes(locationNeedle) || locationNeedle.includes(address)) return true;
      return locationTokens.some((token) => address.includes(token));
    };

     const matchOfferTypes = (row: any) => {
       if (!offerTypeTokens.length) return true;
       const title = String(row?.title || '').toLowerCase();
       const category = String(row?.category || '').toLowerCase();
       const blob = `${title} ${category}`;

       return offerTypeTokens.some((t) => {
         if (!t) return false;
         if (category === t) return true;
         if (t === 'flat discount') return blob.includes('flat') || blob.includes('discount');
         if (t.includes('bogo') || t.includes('buy one get')) return blob.includes('bogo') || blob.includes('buy 1 get 1') || blob.includes('buy one get one');
         if (t === 'percentage off' || t.includes('percent') || t.includes('%')) return blob.includes('%') || blob.includes('percent') || blob.includes('percentage');
         return blob.includes(t);
       });
     };

    const matchesSelectedCategory = (row: any) => {
      if (!categoryNeedle) return true;

      const offerCategory = String(row?.category || '').toLowerCase();
      const merchantCategory = String(row?.merchant?.category || '').toLowerCase();
      const merchantSubCategory = String(row?.merchant?.subCategory || '').toLowerCase();

      const normalizedOfferCategory = normalizeCategoryToken(offerCategory);
      const normalizedMerchantCategory = normalizeCategoryToken(merchantCategory);
      const normalizedMerchantSubCategory = normalizeCategoryToken(merchantSubCategory);

      return (
        offerCategory === categoryNeedle ||
        merchantCategory === categoryNeedle ||
        merchantSubCategory === categoryNeedle ||
        normalizedOfferCategory === normalizedCategoryNeedle ||
        normalizedMerchantCategory === normalizedCategoryNeedle ||
        normalizedMerchantSubCategory === normalizedCategoryNeedle
      );
    };

    let offerRows: any[] = [];
    try {
      offerRows = await this.offerModel
        .find({
          status: { $in: ['under_review', 'approved', 'active'] },
        })
        .select('requestId merchantId merchantName title category businessCategory businessSubCategory totalPrice startDate endDate status createdAt imageUrl selectedProducts')
        .limit(prefetchLimit)
        .maxTimeMS(7000)
        .lean()
        .exec();
    } catch (error: any) {
      this.logger.error(`Nearby offers query failed: ${error?.message || error}`);
      const emptyResult = {
        data: [],
        pagination: {
          page: safePage,
          limit: safeLimit,
          total: 0,
          pages: 0,
        },
      };

      await this.writeCache(cacheKey, emptyResult, this.cacheTtlSeconds.nearbyOffers);
      return emptyResult;
    }

    if (!offerRows.length) {
      const emptyResult = {
        data: [],
        pagination: {
          page: safePage,
          limit: safeLimit,
          total: 0,
          pages: 0,
        },
      };

      await this.writeCache(cacheKey, emptyResult, this.cacheTtlSeconds.nearbyOffers);
      return emptyResult;
    }

    const merchantIds = Array.from(new Set(offerRows.map((row) => String(row.merchantId))));
    let merchants: any[] = [];
    try {
      merchants = await this.merchantModel
        .find({ userId: { $in: merchantIds } })
        .select('userId storeName storeCategory storeSubCategory storeLocation storeLocationLatitude storeLocationLongitude profilePhoto shopPhoto')
        .maxTimeMS(8000)
        .lean()
        .exec();
    } catch (error: any) {
      this.logger.error(`Nearby merchants query failed: ${error?.message || error}`);
      merchants = [];
    }

    const merchantsByUserId = new Map<string, any>(merchants.map((m) => [String(m.userId), m]));

    const now = new Date();

    if (!hasUserCoordinates && !locationNeedle) {
       let normalized = offerRows.map((row) => {
         const merchant = merchantsByUserId.get(String(row.merchantId));
         const pricing = this.computeOfferPricing(row);
         const startsAt = row.startDate ? new Date(row.startDate) : null;
         const endsAt = row.endDate ? new Date(row.endDate) : null;
         const isActiveNow = Boolean(startsAt && endsAt) && startsAt <= now && endsAt >= now;
         return {
           offerId: String(row._id),
           requestId: row.requestId,
           title: row.title,
           category: row.category,
           businessCategory: row.businessCategory || '',
           businessSubCategory: row.businessSubCategory || '',
           // Bug Fix #4: Use default image if missing
           imageUrl: row.imageUrl || DEFAULT_OFFER_IMAGE,
           totalPrice: Number(row.totalPrice || 0),
           displayPrice: pricing.displayPrice,
           discountPercent: pricing.discountPercent,
           startsAt: row.startDate,
           endsAt: row.endDate,
           status: row.status,
           isActiveNow,
           distanceKm: null,
           merchant: {
             merchantId: String(row.merchantId),
             name: merchant?.storeName || row.merchantName || 'Merchant',
             category: merchant?.storeCategory || '',
             subCategory: merchant?.storeSubCategory || '',
             address: merchant?.storeLocation || '',
             latitude: merchant?.storeLocationLatitude || null,
             longitude: merchant?.storeLocationLongitude || null,
             profilePhoto: merchant?.profilePhoto || merchant?.shopPhoto || '',
           },
           selectedProducts: pricing.selectedProducts,
           createdAt: row.createdAt,
         };
       });

      // Only show offers that are within the visibility window (startDate <= now <= endDate).
      if (activeNowOnly) {
        normalized = normalized.filter((row) => row.isActiveNow);
      }

      if (queryNeedle) {
        normalized = normalized.filter((row) => {
          const blob = `${row.title || ''} ${row.category || ''} ${row.merchant.name || ''}`.toLowerCase();
          return blob.includes(queryNeedle);
        });
      }

      normalized = normalized.filter(matchesSelectedCategory);

      if (offerTypeTokens.length) {
        normalized = normalized.filter((row) => matchOfferTypes(row));
      }

      if (topDiscountOnly) {
        normalized = normalized.filter((row) => Number(row.discountPercent || 0) >= 30);
      }

      if (!Number.isNaN(maxPrice) && maxPrice > 0) {
        normalized = normalized.filter((row) => row.displayPrice <= maxPrice);
      }

      normalized.sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());

      const total = normalized.length;
      const pages = Math.ceil(total / safeLimit);
      const start = (safePage - 1) * safeLimit;

      const result = {
        data: normalized.slice(start, start + safeLimit),
        pagination: { page: safePage, limit: safeLimit, total, pages },
      };

      await this.writeCache(cacheKey, result, this.cacheTtlSeconds.nearbyOffers);
      return result;
    }

    let normalized = offerRows.map((row) => {
      const merchant = merchantsByUserId.get(String(row.merchantId));
      const latitude = Number(merchant?.storeLocationLatitude);
      const longitude = Number(merchant?.storeLocationLongitude);
      const hasMerchantCoordinates = this.hasValidMerchantCoordinates(latitude, longitude);

      let distanceKm: number | null = null;
      if (hasUserCoordinates && hasMerchantCoordinates) {
        distanceKm = this.calculateDistanceKm(Number(params.latitude), Number(params.longitude), latitude, longitude);
      }
      const pricing = this.computeOfferPricing(row);

      const startsAt = row.startDate ? new Date(row.startDate) : null;
      const endsAt = row.endDate ? new Date(row.endDate) : null;
      const isActiveNow = Boolean(startsAt && endsAt) && startsAt <= now && endsAt >= now;

       return {
         offerId: String(row._id),
         requestId: row.requestId,
         title: row.title,
         category: row.category,
         businessCategory: row.businessCategory || '',
         businessSubCategory: row.businessSubCategory || '',
         // Bug Fix #4: Use default image if missing
         imageUrl: row.imageUrl || DEFAULT_OFFER_IMAGE,
         totalPrice: Number(row.totalPrice || 0),
         displayPrice: pricing.displayPrice,
         discountPercent: pricing.discountPercent,
         startsAt: row.startDate,
         endsAt: row.endDate,
         status: row.status,
         isActiveNow,
         distanceKm,
         merchant: {
           merchantId: String(row.merchantId),
           name: merchant?.storeName || row.merchantName || 'Merchant',
           category: merchant?.storeCategory || '',
           subCategory: merchant?.storeSubCategory || '',
           address: merchant?.storeLocation || '',
           latitude: hasMerchantCoordinates ? latitude : null,
           longitude: hasMerchantCoordinates ? longitude : null,
           profilePhoto: merchant?.profilePhoto || merchant?.shopPhoto || '',
         },
         selectedProducts: pricing.selectedProducts,
         createdAt: row.createdAt,
       };
    });

    // Only show offers that are within the visibility window (startDate <= now <= endDate).
    if (activeNowOnly) {
      normalized = normalized.filter((row) => row.isActiveNow);
    }

    if (hasUserCoordinates && !locationNeedle) {
      normalized = normalized.filter((row) => {
        if (row.distanceKm === null) {
          return true;
        }
        return row.distanceKm <= safeRadiusKm;
      });
    }

    if (locationNeedle) {
      normalized = normalized.filter((row) => locationMatchesAddress(row.merchant.address || ''));
    }

    if (queryNeedle) {
      normalized = normalized.filter((row) => {
        const searchBlob = [row.title, row.category, row.merchant.name, row.merchant.address].filter(Boolean).join(' ').toLowerCase();
        return searchBlob.includes(queryNeedle);
      });
    }

    normalized = normalized.filter(matchesSelectedCategory);

    if (offerTypeTokens.length) {
      normalized = normalized.filter((row) => matchOfferTypes(row));
    }

    if (topDiscountOnly) {
      normalized = normalized.filter((row) => Number(row.discountPercent || 0) >= 30);
    }

    if (!Number.isNaN(maxPrice) && maxPrice > 0) {
      normalized = normalized.filter((row) => {
        const hasProductPrices = Array.isArray(row.selectedProducts) && row.selectedProducts.length > 0;
        if (!hasProductPrices) return true;
        return row.displayPrice <= maxPrice;
      });
    }

    if (sortBy === 'price_asc') {
      normalized.sort((a, b) => a.displayPrice - b.displayPrice);
    } else if (sortBy === 'price_desc') {
      normalized.sort((a, b) => b.displayPrice - a.displayPrice);
    } else if (sortBy === 'newest') {
      normalized.sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
    } else if (hasUserCoordinates) {
      normalized.sort((a, b) => {
        const distanceA = a.distanceKm ?? Number.MAX_SAFE_INTEGER;
        const distanceB = b.distanceKm ?? Number.MAX_SAFE_INTEGER;
        return distanceA - distanceB;
      });
    } else {
      normalized.sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
    }

    const total = normalized.length;
    const pages = Math.ceil(total / safeLimit);
    const start = (safePage - 1) * safeLimit;
    const data = normalized.slice(start, start + safeLimit);

    const result = { data, pagination: { page: safePage, limit: safeLimit, total, pages } };
    await this.writeCache(cacheKey, result, this.cacheTtlSeconds.nearbyOffers);
    return result;
  }

  // Background job: activate offers whose startDate has arrived, expire offers whose endDate passed
  private async syncOfferStatuses() {
    const now = new Date();
    // Activate offers where startDate <= now <= endDate and status is under_review or approved
    try {
      const activateResult = await this.offerModel.updateMany(
        {
          status: { $in: [OfferPromotionStatus.UNDER_REVIEW, OfferPromotionStatus.APPROVED] },
          startDate: { $lte: now },
          endDate: { $gte: now },
        },
        { $set: { status: OfferPromotionStatus.ACTIVE, isActive: true } },
      ).exec();

      const activated = Number((activateResult && (activateResult as any).modifiedCount) || 0);
      if (activated > 0) {
        this.logger.log(`[Offers] Activated ${activated} offers`);
        // Clear nearby cache so newly active offers appear quickly
        await this.clearCache('golo:offers:nearby:*');
      }
    } catch (err: any) {
      this.logger.warn('[Offers] Activation pass failed: ' + (err?.message || err));
    }

    // Expire offers where endDate < now and status is not expired
    try {
      const expireResult = await this.offerModel.updateMany(
        {
          status: { $ne: OfferPromotionStatus.EXPIRED },
          endDate: { $lt: now },
        },
        { $set: { status: OfferPromotionStatus.EXPIRED, isActive: false } },
      ).exec();

      const expired = Number((expireResult && (expireResult as any).modifiedCount) || 0);
      if (expired > 0) {
        this.logger.log(`[Offers] Expired ${expired} offers`);
        await this.clearCache('golo:offers:nearby:*');
      }
    } catch (err: any) {
      this.logger.warn('[Offers] Expiration pass failed: ' + (err?.message || err));
    }
  }
}
