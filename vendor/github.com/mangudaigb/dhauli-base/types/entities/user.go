package entities

type User struct {
	ID        string `json:"id,omitempty" bson:"_id,omitempty"`
	Name      string `json:"name,omitempty" bson:"name,omitempty"`
	Nick      string `json:"nick,omitempty" bson:"nick,omitempty"`
	FirstName string `json:"firstName,omitempty" bson:"firstName,omitempty"`
	LastName  string `json:"lastName,omitempty" bson:"lastName,omitempty"`
	Avatar    string `json:"avatar,omitempty" bson:"avatar,omitempty"`
	Email     string `json:"email,omitempty" bson:"email,omitempty"`
}

type UserStub struct {
	ID   string `json:"id,omitempty" bson:"_id,omitempty"`
	Nick string `json:"nick,omitempty" bson:"nick,omitempty"`
	Name string `json:"name,omitempty" bson:"name,omitempty"`
}
