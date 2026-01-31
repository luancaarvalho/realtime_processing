from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    {"name": "demo5-topic", "num_partitions": 3, "replication_factor": 1},
]

TIMEOUT_SECONDS = 20


def build_admin():
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def create_topics(admin, topics):
    new_topics = [
        NewTopic(t["name"], num_partitions=t["num_partitions"], replication_factor=t["replication_factor"])
        for t in topics
    ]
    fs = admin.create_topics(new_topics)

    created = []
    skipped = []
    failed = []

    for name, f in fs.items():
        try:
            f.result(timeout=TIMEOUT_SECONDS)
            created.append(name)
        except Exception as e:
            msg = str(e)
            if "TOPIC_ALREADY_EXISTS" in msg or "TopicExists" in msg or "already exists" in msg.lower():
                skipped.append(name)
            else:
                failed.append((name, msg))

    return created, skipped, failed


def main():
    admin = build_admin()
    created, skipped, failed = create_topics(admin, TOPICS)

    if created:
        print("Criados:", ", ".join(created))
    if skipped:
        print("Já existiam:", ", ".join(skipped))
    if failed:
        print("Falharam:")
        for name, err in failed:
            print(f"- {name}: {err}")


if __name__ == "__main__":
    main()
