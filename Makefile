prozess: minikafka/*.c minikafka/*.h
	$(CC) minikafka/*.c -O2 -o $@
