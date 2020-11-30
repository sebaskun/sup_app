# -*- coding: utf-8 -*-
from django.contrib.auth import authenticate
from django.utils.translation import ugettext_lazy as _
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework import serializers
from couchdb import Server
from django.conf import settings
import urllib.parse
import http.client
import json

class SpecialAuthTokenSerializer(AuthTokenSerializer):
    def validate(self, attrs):
        username = attrs.get('username')
        password = attrs.get('password')

        if username and password:
            try:
                # get the user roles
                conn = http.client.HTTPConnection(getattr(settings, 'COUCHDB_SERVER'))
                body = urllib.parse.urlencode({'username': username, 'password': password})
                headers = {
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                conn.request('POST', '/_session', body, headers)
                the_response = conn.getresponse().read().decode('utf-8')
                attrs['roles'] = json.loads(the_response)['roles']
                print('the roles', attrs['roles'])

                # check if the user has the role "admin"
                harcoded_role = 'normal'
                if harcoded_role in attrs['roles']:
                    print('the user need an OTP auth')
                    
                
                else:
                    print('nope, not an admin')

                return attrs
            except Exception as e:
                print('exception', e)
                msg = _('No se pudo establecer conección con Couch')
                raise serializers.ValidationError(msg, code='authorization')

        else:
            msg = _('Must include "username" and "password".')
            raise serializers.ValidationError(msg, code='authorization')

        return 'ok'