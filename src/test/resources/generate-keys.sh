#!/usr/bin/env bash

rm *.jks

# Server keystore
keytool -genkeypair -alias hotrod -keyalg RSA -keysize 2048 -dname "CN=Server,OU=Infinispan,O=Red Hat,L=Europe,ST=,C=UK" -keystore keystore_server.jks -keypass "secret" -storepass "secret" -validity 3650

# Generate client keystore
keytool -genkeypair -alias clientkey -keyalg RSA -keysize 2048 -dname "CN=Client,OU=Infinispan,O=Red Hat,L=Europe,ST=,C=UK" -keypass "secret" -keystore keystore_client.jks -storepass "secret" -validity 3650

# Export server cert
keytool -exportcert -alias hotrod -file server.cer -keystore keystore_server.jks -storepass secret

# Export client certificate
keytool -exportcert -rfc -alias clientkey -file client.cer -keypass secret -keystore keystore_client.jks -storepass secret

# Import client certificate
keytool -importcert -noprompt -alias clientcert -file client.cer -keystore ca.jks -storepass secret

# Import server certificate
keytool -importcert -noprompt -alias servercert -file server.cer -keystore truststore_client.jks -storepass secret

rm *.cer