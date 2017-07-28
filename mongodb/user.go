package mongodb

// User model
type User struct {
	ID    int    `json:"_id"`
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
}
