package kafka

import (
 "encoding/json"
 "errors"
 "fmt"
 "strings"
 "testing"
 "time"
 ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestR2StructuredNativeBoundary(t *testing.T) {
 for _, tc := range []struct{code ckafka.ErrorCode;api string;want int}{
  {ckafka.ErrTopicAuthorizationFailed,"produce",29}, {ckafka.ErrTopicAuthorizationFailed,"consume",29},
  {ckafka.ErrGroupAuthorizationFailed,"group_join",30}, {ckafka.ErrClusterAuthorizationFailed,"produce",31},
  {ckafka.ErrGroupAuthorizationFailed,"produce",0}, {ckafka.ErrTopicAuthorizationFailed,"group_join",0},
  {ckafka.ErrTimedOut,"produce",0}, {ckafka.ErrSaslAuthenticationFailed,"consume",0},
 } {
  t.Run(fmt.Sprintf("%d_%s",int(tc.code),tc.api),func(t *testing.T){
   err:=NewXk6KafkaError(writerError,"CANARY",ckafka.NewError(tc.code,"OFFLINE_SECRET_CANARY",false))
   got:=r2ErrorBoundary(err,tc.api); if got["code"]!=tc.want {t.Fatalf("wrong safe code: %v",got["code"])}
   raw,_:=json.Marshal(got);if strings.Contains(string(raw),"CANARY"){t.Fatal("secret exported")}
  })
 }
 for _,e:=range []error{nil,errors.New("Topic authorization failed password=OFFLINE_SECRET_CANARY")} {
  if r2ErrorBoundary(e,"produce")["code"]!=0 {t.Fatal("message string must not classify")}
 }
}
func TestR2ActualConfigMapping(t *testing.T){
 writer,err:=writerConfigToConfluentConfigMap(&WriterConfig{Brokers:[]string{"offline.invalid:9093"},RequiredAcks:-1,WriteTimeout:10*time.Second,ReadTimeout:10*time.Second,Compression:""})
 if err!=nil {t.Fatal("mapping failed")};if writer["acks"]!="all"||writer["message.timeout.ms"]!=10000 {t.Fatal("producer mapping drift")}
 reader,err:=readerConfigToConfluentConfigMap(&ReaderConfig{Brokers:[]string{"offline.invalid:9093"},GroupID:"kaas-sbx-a-events-group",CommitInterval:0,MaxBytes:4194304})
 if err!=nil {t.Fatal("mapping failed")};if reader["enable.auto.commit"]!=false {t.Fatal("automatic commit enabled")}
 // Omitted is not a demonstrated safe explicit setting. Record this native capability gap.
 if _,ok:=writer["enable.idempotence"];ok {t.Fatal("source mapping changed; reassess diagnostics")}
}
