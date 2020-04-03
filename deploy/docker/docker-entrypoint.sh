#!/bin/sh
if [[ ! -z "$DEBUG" ]]; then
    set -ex
else
    set -e
fi

KUIPER_HOME=${KUIPER_HOME:-"/kuiper"}

CONFIG="$KUIPER_HOME/etc/mqtt_source.yaml"

if [ ! -z "$MQTT_BROKER_ADDRESS" ]; then
    sed -i '/default:/ ,/servers/{/servers/d}' $CONFIG
    sed -i "/default:/a\  servers: [$MQTT_BROKER_ADDRESS]" $CONFIG
    echo "mqtt.default.servers = $MQTT_BROKER_ADDRESS"
fi

if [ ! -z "$MQTT_BROKER_SHARED_SUBSCRIPTION" ]; then
    sed -i '/default:/ ,/sharedSubscription/{/sharedSubscription/d}' $CONFIG
    sed -i "/default:/a\  sharedSubscription: $MQTT_BROKER_SHARED_SUBSCRIPTION" $CONFIG
    echo "mqtt.default.sharedSubscription = $MQTT_BROKER_SHARED_SUBSCRIPTION"
fi

if [ ! -z "$MQTT_BROKER_QOS" ]; then
    sed -i '/default:/ ,/qos/{/qos/d}' $CONFIG
    sed -i "/default:/a\  qos: $MQTT_BROKER_QOS" $CONFIG
    echo "mqtt.default.qos = $MQTT_BROKER_QOS"
fi

if [ ! -z "$MQTT_BROKER_USERNAME" ]; then
    sed -i '/default:/ ,/username/{/username/d}' $CONFIG
    sed -i "/default:/a\  username: $MQTT_BROKER_USERNAME" $CONFIG
    echo "mqtt.default.username = $MQTT_BROKER_USERNAME"
fi

if [ ! -z "$MQTT_BROKER_PASSWORD" ]; then
    sed -i '/default:/ ,/password/{/password/d}' $CONFIG
    sed -i "/default:/a\  password: $MQTT_BROKER_PASSWORD" $CONFIG
    echo "mqtt.default.password = $MQTT_BROKER_PASSWORD"
fi

if [ ! -z "$MQTT_BROKER_CER_PATH" ]; then
    sed -i '/default:/ ,/certificationPath/{/certificationPath/d}' $CONFIG
    sed -i "/default:/a\  certificationPath: $MQTT_BROKER_CER_PATH" $CONFIG
    echo "mqtt.default.certificationPath = $MQTT_BROKER_CER_PATH"
fi

if [ ! -z "$MQTT_BROKER_KEY_PATH" ]; then
    sed -i '/default:/ ,/privateKeyPath/{/privateKeyPath/d}' $CONFIG
    sed -i "/default:/a\  privateKeyPath: $MQTT_BROKER_KEY_PATH" $CONFIG
    echo "mqtt.default.privateKeyPath = $MQTT_BROKER_KEY_PATH"
fi

EDGEX_CONFIG="$KUIPER_HOME/etc/sources/edgex.yaml"

if [ ! -z "$EDGEX_PROTOCOL" ]; then
    sed -i '/default:/ ,/protocol/{/protocol/d}' $EDGEX_CONFIG
    sed -i "/default:/a\  protocol: $EDGEX_PROTOCOL" $EDGEX_CONFIG
    echo "edgex.default.protocol = $EDGEX_PROTOCOL"
fi

if [ ! -z "$EDGEX_SERVER" ]; then
    sed -i '/default:/ ,/server/{/server/d}' $EDGEX_CONFIG
    sed -i "/default:/a\  server: $EDGEX_SERVER" $EDGEX_CONFIG
    echo "edgex.default.server = $EDGEX_SERVER"
fi

if [ ! -z "$EDGEX_PORT" ]; then
    sed -i '/default:/ ,/port/{/port/d}' $EDGEX_CONFIG
    sed -i "/default:/a\  port: $EDGEX_PORT" $EDGEX_CONFIG
    echo "edgex.default.port = $EDGEX_PORT"
fi

if [ ! -z "$EDGEX_TOPIC" ]; then
    sed -i '/default:/ ,/topic/{/topic/d}' $EDGEX_CONFIG
    sed -i "/default:/a\  topic: $EDGEX_TOPIC" $EDGEX_CONFIG
    echo "edgex.default.topic = $EDGEX_TOPIC"
fi

if [ ! -z "$EDGEX_SERVICE_SERVER" ]; then
    sed -i '/default:/ ,/serviceServer/{/serviceServer/d}' $EDGEX_CONFIG
    sed -i "/default:/a\  serviceServer: $EDGEX_SERVICE_SERVER" $EDGEX_CONFIG
    echo "edgex.default.serviceServer = $EDGEX_SERVICE_SERVER"
fi

exec "$@"
