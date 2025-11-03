package requests

type ContextRequest struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name" binding:"required"`
	Description string   `json:"description,omitempty"`
	Content     string   `json:"content" binding:"required"`
	Tags        []string `json:"tags,omitempty"`
}
