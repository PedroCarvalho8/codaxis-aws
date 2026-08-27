import boto3
import cfnresponse

s3 = boto3.client("s3")

def handler(event, context):
    props = event["ResourceProperties"]
    bucket, key = props["Bucket"], props["Key"]
    uri = f"s3://{bucket}/{key}"
    try:
        if event["RequestType"] == "Delete":
            # O bucket de dados tem DeletionPolicy Retain, mas pode
            # ter sido removido a mao. Falhar aqui travaria o delete
            # da pilha.
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass
        else:
            extra = {}
            for campo in ("ContentType", "CacheControl"):
                if props.get(campo):
                    extra[campo] = props[campo]
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=props["Content"].encode("utf-8"),
                **extra,
            )
        cfnresponse.send(
            event, context, cfnresponse.SUCCESS, {"Uri": uri}, uri
        )
    except Exception as exc:
        cfnresponse.send(
            event, context, cfnresponse.FAILED, {"Erro": str(exc)}, uri
        )
