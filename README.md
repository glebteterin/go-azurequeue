# Go client for Azure Service Bus Queue

[![GoDoc](https://godoc.org/github.com/g-rad/go-azurequeue?status.svg)](http://godoc.org/github.com/g-rad/go-azurequeue)
[![Build status](https://ci.appveyor.com/api/projects/status/0qlr2s2x2p91d644?svg=true)](https://ci.appveyor.com/project/g-rad/go-azurequeue)
[![codecov](https://codecov.io/gh/g-rad/go-azurequeue/branch/master/graph/badge.svg)](https://codecov.io/gh/g-rad/go-azurequeue)

# Install and Use:

### Install

```sh
$ go get -u github.com/g-rad/go-azurequeue
```

or if you use dep, within your repo run:

```sh
$ dep ensure -add github.com/g-rad/go-azurequeue
```

If you need to install Go, follow [the official instructions](https://golang.org/dl/).

### Use


##### Init Client

```go

import "github.com/g-rad/go-azurequeue"

cli := queue.QueueClient{
  Namespace:  "my-test",
  KeyName:    "RootManageSharedAccessKey",
  KeyValue:   "ErCWbtgArb55Tqqu9tXgdCtopbZ44pMH01sjpMrYGrE=",
  QueueName:  "my-queue",
  Timeout:    60,
}
```

##### Send Message

```go
// create message
msg := queue.NewMessage(]byte("Hello!"))

msg.Properties.Set("Property1", "Value1")
msg.Properties.Set("Property2", "Value2")

// send message
cli.SendMessage(&msg)
```

##### Receive Next Message

```go
msg, err := cli.GetMessage()
```

##### Unlock Message
If you failed to process a message, unlock it for processing by other receivers.
```go
cli.UnlockMessage(&msg)
```

##### Delete Message
This operation completes the processing of a locked message and deletes it from the queue.
```go
cli.DeleteMessage(&msg)
```