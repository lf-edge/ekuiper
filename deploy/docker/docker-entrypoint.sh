#!/bin/sh
if [[ ! -z "$DEBUG" ]]; then
    set -ex
else
    set -e
fi

KUIPER_HOME="/kuiper"

CONFIG="$KUIPER_HOME/etc/mqtt_source.yaml"

if [ ! -z "$MQTT_BROKER_ADDRESS" ]; then
    sed -i '/default:/ ,/servers/{/servers/d}' $CONFIG
    sed -i "/default:/a\  servers: [$MQTT_BROKER_ADDRESS]" $CONFIG
    echo "default.servers = $MQTT_BROKER_ADDRESS"
fi

if [ ! -z "$MQTT_BROKER_SHARED_SUBSCRIPTION" ]; then
    sed -i '/default:/ ,/sharedSubscription/{/sharedSubscription/d}' $CONFIG
    sed -i "/default:/a\  sharedSubscription: $MQTT_BROKER_SHARED_SUBSCRIPTION" $CONFIG
    echo "default.sharedSubscription = $MQTT_BROKER_SHARED_SUBSCRIPTION"
fi

if [ ! -z "$MQTT_BROKER_QOS" ]; then
    sed -i '/default:/ ,/qos/{/qos/d}' $CONFIG
    sed -i "/default:/a\  qos: $MQTT_BROKER_QOS" $CONFIG
    echo "default.qos = $MQTT_BROKER_QOS"
fi

if [ ! -z "$MQTT_BROKER_USERNAME" ]; then
    sed -i '/default:/ ,/username/{/username/d}' $CONFIG
    sed -i "/default:/a\  username: $MQTT_BROKER_USERNAME" $CONFIG
    echo "default.username = $MQTT_BROKER_USERNAME"
fi

if [ ! -z "$MQTT_BROKER_PASSWORD" ]; then
    sed -i '/default:/ ,/password/{/password/d}' $CONFIG
    sed -i "/default:/a\  password: $MQTT_BROKER_PASSWORD" $CONFIG
    echo "default.password = $MQTT_BROKER_PASSWORD"
fi

if [ ! -z "$MQTT_BROKER_CER_PATH" ]; then
    sed -i '/default:/ ,/certificationPath/{/certificationPath/d}' $CONFIG
    sed -i "/default:/a\  certificationPath: $MQTT_BROKER_CER_PATH" $CONFIG
    echo "default.certificationPath = $MQTT_BROKER_CER_PATH"
fi

if [ ! -z "$MQTT_BROKER_KEY_PATH" ]; then
    sed -i '/default:/ ,/privateKeyPath/{/privateKeyPath/d}' $CONFIG
    sed -i "/default:/a\  privateKeyPath: $MQTT_BROKER_KEY_PATH" $CONFIG
    echo "default.privateKeyPath = $MQTT_BROKER_KEY_PATH"
fi

exec "$@"
