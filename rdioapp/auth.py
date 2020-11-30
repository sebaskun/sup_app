from rest_framework import status, HTTP_HEADER_ENCODING, exceptions
from django.utils.translation import gettext as _
from rest_framework.authentication import SessionAuthentication, TokenAuthentication, get_authorization_header
from rest_framework.response import Response
from rest_framework.authtoken.views import ObtainAuthToken
from .serializers import SpecialAuthTokenSerializer
from django.contrib.auth.models import User
from .models import Token
import datetime
from django.utils import timezone
from django.conf import settings


# File to define custom authentication classes
class Special_TokenAuthentication(TokenAuthentication):
    def authenticate(self, request):
        auth = get_authorization_header(request).split()
        if not auth or auth[0].lower() != self.keyword.lower().encode():
            return None

        if len(auth) == 1:
            msg = _('Invalid token header. No credentials provided.')
            raise exceptions.AuthenticationFailed(msg)
        elif len(auth) > 2:
            msg = _('Invalid token header. Token string should not contain spaces.')
            raise exceptions.AuthenticationFailed(msg)

        try:
            token = auth[1].decode()

        except (UnicodeError, IndexError) as e:
            msg = _('Invalid token header. Token string should not contain invalid characters.')
            raise exceptions.AuthenticationFailed(msg)

        return self.authenticate_credentials(token)

    def authenticate_credentials(self, key):
        model = self.get_model()
        try:
            token = model.objects.select_related('user').get(key=key)

            # Check if Token has more than X hours of created. If so, proceed to renew
            token_duration = getattr(settings, 'TOKEN_DURATION')
            
            # if token.created + datetime.timedelta(minutes=2) > timezone.now():
            if token.created + datetime.timedelta(hours=token_duration) > timezone.now():
                print('Valid JWT Token') 
            else:
                print('JWT Token expired, proceeding to renew...')
                token.delete()
                raise exceptions.AuthenticationFailed(_('The token has expired, please re-authenticate'))
            
        except model.DoesNotExist:
            raise exceptions.AuthenticationFailed(_('Invalid token.'))

        if not token.user.is_active:
            raise exceptions.AuthenticationFailed(_('User inactive or deleted.'))

        return (token.user, token)

    def get_model(self):
        if self.model is not None:
            return self.model
        from .models import Token
        return Token

class special_obtain_auth_token(ObtainAuthToken):
    serializer_class = SpecialAuthTokenSerializer

    def post(self, request, *args, **kwargs):
        # the serializer tries to authenticate with Couch and retrieve the roles of the user
        serializer = self.serializer_class(data=request.data,
                                        context={'request': request})
        serializer.is_valid(raise_exception=True)

        # the user may have multiple roles, so we include them all in the compund token, separated by a dot
        roles = serializer.validated_data['roles']
        roles_string = ''
        for item in roles:
            roles_string = item + '.'
        username = serializer.validated_data['username']
        user = User.objects.filter(username='admin')[0]
        token, created = Token.objects.get_or_create(user=user, couch_user=username, couch_roles=roles_string)

        compound_token = token.key
        return Response({'token': compound_token})