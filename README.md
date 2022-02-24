# PoC RabbitMQ Priority Queue

The PoC used to test [Priority Queue support in RabbitMQ](https://www.rabbitmq.com/priority.html).

## Producer

```
python producer.py --queue=test_queue --payload="/" -n 1000 -nice 1
python producer.py --queue=test_queue --payload="^" -n 1000 -nice 10
python producer.py --queue=test_queue --payload="@" -n 1000 -nice 99
```

## Consumer

```
python consumer.py --queue=test_queue
```

## Conclusion

Example of output:

```
/ / / / / / / / / / / / / / / / / / / / / / / / / / @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ ^ ^ ^ ^ ^ ^ ^ ^ ^ @ @ @ @ @ @ @ @ ^ ^ ^ ^ ^ ^ ^ ^ / / / / / / / / /
```

The messages with higher priority (`@`) is processed before the messages with the lower one (`^` and `/`).

