import base64

def retail_notification(event, context):
    message = base64.b64decode(event['data']).decode('utf-8')

    print("===== PUBSUB MESSAGE RECEIVED =====")
    print(message)
    print("===== PIPELINE COMPLETED =====")
