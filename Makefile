run: slack-file-cleaner
	./run

slack-file-cleaner: main.go
	go build
