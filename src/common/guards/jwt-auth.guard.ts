import { Injectable, ExecutionContext, UnauthorizedException, Logger } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import { getAccessTokenFromRequest } from '../utils/auth-token.util';

@Injectable()
export class JwtAuthGuard extends AuthGuard('jwt') {
  private readonly logger = new Logger(JwtAuthGuard.name);

  canActivate(context: ExecutionContext) {
    const request = context.switchToHttp().getRequest();
    const token = getAccessTokenFromRequest(request);
    
    this.logger.debug(`[JWT Guard] Token source: ${token ? 'Present' : 'Missing'}`);

    if (!token && request?.headers?.authorization) {
      const parts = request.headers.authorization.split(' ');
      if (parts.length !== 2 || parts[0].toLowerCase() !== 'bearer') {
        this.logger.warn(`[JWT Guard] Invalid token format: ${parts[0]}`);
      }
    }
    
    return super.canActivate(context);
  }

  handleRequest(err, user, info) {
    if (err) {
      this.logger.error(`[JWT Guard] Error: ${err.message}`);
      throw err;
    }
    
    if (!user) {
      this.logger.warn(`[JWT Guard] No user found. Info: ${info?.message}`);
      throw new UnauthorizedException('Authentication required');
    }
    
    this.logger.debug(`[JWT Guard] User authenticated: ${user.id}`);
    return user;
  }
}