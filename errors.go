package queue

import "fmt"

type NoMessagesAvailableError struct {
	Code int
	Body string
}

func (e NoMessagesAvailableError) Error() string {
	return "No messages available within the specified timeout period"
}

type BadRequestError struct {
	Code int
	Body string
}

func (e BadRequestError) Error() string {
	return "Bad createRequest"
}

type NotAuthorizedError struct {
	Code int
	Body string
}

func (e NotAuthorizedError) Error() string {
	return "Authorization failure"
}

type MessageDontExistError struct {
	Code int
	Body string
}

func (e MessageDontExistError) Error() string {
	return "No message was found with the specified MessageId or LockToken."
}

type QueueDontExistError struct {
	Code int
	Body string
}

func (e QueueDontExistError) Error() string {
	return "Specified queue or subscription does not exist"
}

type InternalError struct {
	Code int
	Body string
}

func (e InternalError) Error() string {
	return "Internal Error"
}

func wrap(err error, message string) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(message + ": " + err.Error())
}