from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any

import jwt
from fastapi import Depends, Header, Request

from .config import AuthConfig
from .errors import ForbiddenError, UnauthorizedError


class AccessLevel(str, Enum):
    VIEWER = "viewer"
    ANALYST = "analyst"
    ADMIN = "admin"


ACCESS_ORDER = {
    AccessLevel.VIEWER: 1,
    AccessLevel.ANALYST: 2,
    AccessLevel.ADMIN: 3,
}


@dataclass(frozen=True, slots=True)
class UserIdentity:
    user_id: str
    access_level: AccessLevel
    claims: dict[str, Any]


class JWTAuthenticator:
    def __init__(self, config: AuthConfig) -> None:
        self._config = config
        self._jwk_client = jwt.PyJWKClient(config.jwks_url) if not config.disabled and config.jwks_url else None

    def authenticate(self, authorization: str | None) -> UserIdentity:
        if self._config.disabled:
            return UserIdentity(
                user_id="dev-user",
                access_level=AccessLevel.ADMIN,
                claims={"sub": "dev-user", "role": AccessLevel.ADMIN.value},
            )
        if not authorization:
            raise UnauthorizedError("Отсутствует заголовок Authorization")
        if not authorization.startswith("Bearer "):
            raise UnauthorizedError("Неверный формат заголовка Authorization")

        token = authorization.removeprefix("Bearer ").strip()
        if not token:
            raise UnauthorizedError("Пустой JWT-токен")

        try:
            assert self._jwk_client is not None
            signing_key = self._jwk_client.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=self._config.audience,
                issuer=self._config.issuer,
            )
        except jwt.ExpiredSignatureError as exc:
            raise UnauthorizedError("JWT-токен просрочен") from exc
        except jwt.InvalidTokenError as exc:
            raise UnauthorizedError("JWT-токен невалиден") from exc

        user_id = str(claims.get("sub") or "").strip()
        if not user_id:
            raise UnauthorizedError("JWT-токен не содержит sub")

        access_level = self._extract_access_level(claims)
        return UserIdentity(user_id=user_id, access_level=access_level, claims=dict(claims))

    def _extract_access_level(self, claims: dict[str, Any]) -> AccessLevel:
        roles_value = claims.get(self._config.roles_claim)
        role_value = claims.get(self._config.role_claim)
        role_names: list[str] = []
        if isinstance(roles_value, list):
            role_names.extend(str(item) for item in roles_value)
        elif isinstance(roles_value, str):
            role_names.append(roles_value)
        if role_value:
            role_names.append(str(role_value))

        resolved = AccessLevel.VIEWER
        for role_name in role_names:
            normalized = role_name.strip().lower()
            if normalized == AccessLevel.ADMIN.value:
                resolved = AccessLevel.ADMIN
                break
            if normalized == AccessLevel.ANALYST.value:
                resolved = AccessLevel.ANALYST
            elif normalized == AccessLevel.VIEWER.value:
                resolved = max((resolved, AccessLevel.VIEWER), key=lambda level: ACCESS_ORDER[level])
        return resolved


def build_auth_dependency(authenticator: JWTAuthenticator):
    async def get_current_user(  # type: ignore[no-untyped-def]
        request: Request,
        authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> UserIdentity:
        user = authenticator.authenticate(authorization)
        request.state.current_user = user
        request.state.user_id = user.user_id
        request.state.access_level = user.access_level.value
        return user

    return get_current_user


def require_access(level: AccessLevel):
    async def dependency(user: UserIdentity = Depends(_get_current_user_from_state)) -> UserIdentity:
        if ACCESS_ORDER[user.access_level] < ACCESS_ORDER[level]:
            raise ForbiddenError()
        return user

    return dependency


async def _get_current_user_from_state(request: Request) -> UserIdentity:
    user = getattr(request.state, "current_user", None)
    if user is None:
        raise UnauthorizedError()
    return user
