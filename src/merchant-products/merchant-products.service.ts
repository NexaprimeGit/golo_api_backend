import {
  ForbiddenException,
  Injectable,
  Logger,
  Optional,
  NotFoundException,
  OnModuleInit,
} from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { FilterQuery, Model } from 'mongoose';
import { KafkaService } from '../kafka/kafka.service';
import { KAFKA_TOPICS } from '../common/constants/kafka-topics';
import { RedisService } from '../common/services/redis.service';
import { CreateMerchantProductDto } from './dto/create-merchant-product.dto';
import { ListMerchantProductsDto } from './dto/list-merchant-products.dto';
import { UpdateMerchantProductDto } from './dto/update-merchant-product.dto';
import {
  MerchantProduct,
  MerchantProductDocument,
} from './schemas/merchant-product.schema';

@Injectable()
export class MerchantProductsService implements OnModuleInit {
  private readonly logger = new Logger(MerchantProductsService.name);

  constructor(
    @InjectModel(MerchantProduct.name)
    private readonly merchantProductModel: Model<MerchantProductDocument>,
    private readonly redisService: RedisService,
    @Optional() private readonly kafkaService?: KafkaService,
  ) {}

  private cacheListKey(merchantId: string, query: ListMerchantProductsDto): string {
    return `golo:merchant:products:list:${merchantId}:${query.page || 1}:${query.limit || 10}:${(query.search || '').trim().toLowerCase()}`;
  }

  private cacheItemKey(merchantId: string, productId: string): string {
    return `golo:merchant:products:item:${merchantId}:${productId}`;
  }

  private async invalidateMerchantProductCache(merchantId: string): Promise<void> {
    await this.redisService.deleteByPattern(`golo:merchant:products:list:${merchantId}:*`);
    await this.redisService.deleteByPattern(`golo:merchant:products:item:${merchantId}:*`);
  }

  async onModuleInit() {
    if (this.kafkaService) {
      this.logger.log('Kafka service connected for MerchantProductsService');
    }
  }

  private deriveStatus(stockQuantity: number):
    | 'In Stock'
    | 'Low Stock'
    | 'Out of Stock' {
    if (stockQuantity <= 0) return 'Out of Stock';
    if (stockQuantity <= 10) return 'Low Stock';
    return 'In Stock';
  }

  async create(merchantId: string, dto: CreateMerchantProductDto) {
    const payload = {
      merchantId,
      name: dto.name.trim(),
      category: dto.category.trim(),
      stockQuantity: dto.stockQuantity,
      price: dto.price,
      description: dto.description?.trim() || '',
      images: dto.images || [],
      status: this.deriveStatus(dto.stockQuantity),
    };

    const product = await this.merchantProductModel.create(payload);
    await this.invalidateMerchantProductCache(merchantId);

    if (this.kafkaService) {
      await this.kafkaService.emit(KAFKA_TOPICS.MERCHANT_PRODUCT_CREATED, {
        merchantId,
        productId: String(product._id),
        name: product.name,
        category: product.category,
        price: product.price,
        stockQuantity: product.stockQuantity,
      });
    }

    return {
      success: true,
      message: 'Product created successfully',
      data: this.mapProduct(product),
    };
  }

  async listMyProducts(merchantId: string, query: ListMerchantProductsDto) {
    const cacheKey = this.cacheListKey(merchantId, query);
    const cached = await this.redisService.get<any>(cacheKey);
    if (cached) {
      return cached;
    }

    const page = Number(query.page) || 1;
    const limit = Number(query.limit) || 10;
    const skip = (page - 1) * limit;

    const filter: FilterQuery<MerchantProductDocument> = { merchantId };

    if (query.search?.trim()) {
      const searchRegex = new RegExp(query.search.trim(), 'i');
      filter.$or = [{ name: searchRegex }, { category: searchRegex }];
    }

    const [products, total, totalProducts, lowStockProducts, outOfStockProducts, inventoryAgg] =
      await Promise.all([
        this.merchantProductModel
          .find(filter)
          .sort({ createdAt: -1 })
          .skip(skip)
          .limit(limit)
          .exec(),
        this.merchantProductModel.countDocuments(filter),
        this.merchantProductModel.countDocuments({ merchantId }),
        this.merchantProductModel.countDocuments({
          merchantId,
          stockQuantity: { $gt: 0, $lte: 10 },
        }),
        this.merchantProductModel.countDocuments({ merchantId, stockQuantity: 0 }),
        this.merchantProductModel.aggregate([
          { $match: { merchantId } },
          {
            $group: {
              _id: null,
              total: {
                $sum: { $multiply: ['$price', '$stockQuantity'] },
              },
            },
          },
        ]),
      ]);

    const inventoryValue = inventoryAgg[0]?.total || 0;

    const response = {
      success: true,
      data: {
        products: products.map((product) => this.mapProduct(product)),
        stats: {
          totalProducts,
          inventoryValue,
          lowStockProducts,
          outOfStockProducts,
        },
      },
      pagination: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
    };

    await this.redisService.set(cacheKey, response, 120);
    return response;
  }

  async listProductsByMerchantId(merchantId: string, query: ListMerchantProductsDto) {
    const page = Number(query.page) || 1;
    const limit = Number(query.limit) || 10;
    const skip = (page - 1) * limit;

    const filter: FilterQuery<MerchantProductDocument> = { merchantId };

    if (query.search?.trim()) {
      const searchRegex = new RegExp(query.search.trim(), 'i');
      filter.$or = [{ name: searchRegex }, { category: searchRegex }];
    }

    const [products, total] = await Promise.all([
      this.merchantProductModel
        .find(filter)
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limit)
        .exec(),
      this.merchantProductModel.countDocuments(filter),
    ]);

    return {
      success: true,
      data: {
        products: products.map((product) => this.mapProduct(product)),
      },
      pagination: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
    };
  }

  async getProduct(merchantId: string, productId: string) {
    const cacheKey = this.cacheItemKey(merchantId, productId);
    const cached = await this.redisService.get<any>(cacheKey);
    if (cached) {
      return cached;
    }

    const product = await this.merchantProductModel.findById(productId).exec();
    if (!product) {
      throw new NotFoundException('Product not found');
    }

    if (String(product.merchantId) !== String(merchantId)) {
      throw new ForbiddenException('You can only view your own products');
    }

    const response = {
      success: true,
      data: this.mapProduct(product),
    };

    await this.redisService.set(cacheKey, response, 300);
    return response;
  }

  async getPublicProductById(productId: string) {
    const product = await this.merchantProductModel.findById(productId).exec();
    if (!product) {
      throw new NotFoundException('Product not found');
    }

    return {
      success: true,
      data: this.mapProduct(product),
    };
  }

  async updateProduct(merchantId: string, productId: string, dto: UpdateMerchantProductDto) {
    const product = await this.merchantProductModel.findById(productId).exec();
    if (!product) {
      throw new NotFoundException('Product not found');
    }

    if (String(product.merchantId) !== String(merchantId)) {
      throw new ForbiddenException('You can only update your own products');
    }

    if (typeof dto.name === 'string') {
      product.name = dto.name.trim();
    }
    if (typeof dto.category === 'string') {
      product.category = dto.category.trim();
    }
    if (typeof dto.description === 'string') {
      product.description = dto.description.trim();
    }
    if (typeof dto.price === 'number') {
      product.price = dto.price;
    }
    if (typeof dto.stockQuantity === 'number') {
      product.stockQuantity = dto.stockQuantity;
      product.status = this.deriveStatus(dto.stockQuantity);
    }

    await product.save();
    await this.invalidateMerchantProductCache(merchantId);

    return {
      success: true,
      message: 'Product updated successfully',
      data: this.mapProduct(product),
    };
  }

  async deleteProduct(merchantId: string, productId: string) {
    const product = await this.merchantProductModel.findById(productId).exec();
    if (!product) {
      throw new NotFoundException('Product not found');
    }

    if (String(product.merchantId) !== String(merchantId)) {
      throw new ForbiddenException('You can only delete your own products');
    }

    if (this.kafkaService) {
      await this.kafkaService.emit(KAFKA_TOPICS.MERCHANT_PRODUCT_DELETED, {
        merchantId,
        productId: String(product._id),
        name: product.name,
        category: product.category,
        price: product.price,
        stockQuantity: product.stockQuantity,
      });
    }

    await this.merchantProductModel.findByIdAndDelete(productId).exec();
    await this.invalidateMerchantProductCache(merchantId);

    return {
      success: true,
      message: 'Product deleted successfully',
    };
  }

  private mapProduct(product: MerchantProductDocument) {
    return {
      id: String(product._id),
      productId: String(product._id),
      merchantId: product.merchantId,
      name: product.name,
      category: product.category,
      description: product.description,
      stockQuantity: product.stockQuantity,
      stock: `${product.stockQuantity} units`,
      price: product.price,
      priceLabel: `₹${product.price}`,
      status: product.status,
      image: product.images?.[0] || null,
      images: product.images || [],
      createdAt: product.createdAt,
      updatedAt: product.updatedAt,
    };
  }
}
