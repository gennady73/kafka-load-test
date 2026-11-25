package kafka

// Additive candidate helper. It is NOT exported or called by the legacy JS API.
// Caller must bind a fixed operation stage and keep raw errors out of logging.
import (
 "errors"
 ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func r2ErrorBoundary(err error, api string) map[string]any {
 unknown := map[string]any{"schema":"r2-native-error-1", "code":0, "api":"unknown"}
 if api != "produce" && api != "consume" && api != "group_join" { return unknown }
 var native ckafka.Error
 if !errors.As(err, &native) { return unknown }
 code := native.Code()
 if (code == ckafka.ErrTopicAuthorizationFailed && (api == "produce" || api == "consume")) ||
    (code == ckafka.ErrGroupAuthorizationFailed && (api == "consume" || api == "group_join")) ||
    (code == ckafka.ErrClusterAuthorizationFailed && api == "produce") {
    return map[string]any{"schema":"r2-native-error-1", "code":int(code), "api":api}
 }
 return unknown
}
