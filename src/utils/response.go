package utils


type ResponseMessage struct {
	Data any `json:"data"`
	Code int `json:"code"`
	Message string `json:"message"`
}

type HTTPError struct {
	Data any `json:"data"`
	Code int `json:"code"`
	Message string `json:"message"`
}