from kafka.consumer import OrderConsumer


if __name__ == "__main__":
    consumer = OrderConsumer()
    consumer.run()
