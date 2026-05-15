import { Request } from 'express';

export function getCookieValue(cookieHeader: string | undefined, cookieName: string): string | null {
  if (!cookieHeader) return null;

  const parts = cookieHeader.split(';');
  for (const part of parts) {
    const [rawKey, ...rawValueParts] = part.trim().split('=');
    if (rawKey !== cookieName) continue;
    const value = rawValueParts.join('=').trim();
    if (!value) return null;
    try {
      return decodeURIComponent(value);
    } catch {
      return value;
    }
  }

  return null;
}

export function getAccessTokenFromRequest(request: Request | { headers?: any; handshake?: any }): string | null {
  const authorizationHeader = request?.headers?.authorization;
  if (typeof authorizationHeader === 'string' && authorizationHeader.startsWith('Bearer ')) {
    return authorizationHeader.slice(7);
  }

  const requestAny = request as any;
  const cookieHeader = requestAny?.headers?.cookie || requestAny?.handshake?.headers?.cookie;
  return getCookieValue(cookieHeader, 'accessToken');
}

export function getRefreshTokenFromRequest(request: Request | { headers?: any }): string | null {
  const cookieHeader = (request as any)?.headers?.cookie;
  return getCookieValue(cookieHeader, 'refreshToken');
}