import { Injectable, UnauthorizedException } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { User, UserDocument } from '../schemas/user.schema';
import { getAccessTokenFromRequest } from '../../common/utils/auth-token.util';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    private configService: ConfigService,
    @InjectModel(User.name) private userModel: Model<UserDocument>,
  ) {
    // Get secret from config service
    const secret = configService.get<string>('JWT_SECRET');
    
    if (!secret) {
      throw new Error('JWT_SECRET is not defined in environment variables');
    }

    super({
      jwtFromRequest: ExtractJwt.fromExtractors([
        getAccessTokenFromRequest as any,
        ExtractJwt.fromAuthHeaderAsBearerToken(),
      ]),
      ignoreExpiration: false,
      secretOrKey: secret,
    });
  }

  async validate(payload: any) {
    try {
      if (!payload || !payload.sub) {
        throw new UnauthorizedException('Invalid JWT payload - missing sub claim');
      }
      
      // Handle admin tokens - they come from GOLO_Admin_Backend and have role: 'admin'
      if (payload.role === 'admin') {
        return {
          id: payload.sub,
          email: payload.email,
          role: 'admin',
          name: payload.name || payload.email,
        };
      }
      
      // For regular users, look them up in the database
      const user = await this.userModel.findById(payload.sub).exec();
      if (!user) {
        throw new UnauthorizedException('User not found');
      }
      
      // Convert _id to string to avoid ObjectId issues
      return {
        id: user._id.toString(),
        email: user.email,
        role: user.role,
        name: user.name,
      };
    } catch (error) {
      console.error('[JWT Strategy] Validation failed:', error.message);
      throw error;
    }
  }
}