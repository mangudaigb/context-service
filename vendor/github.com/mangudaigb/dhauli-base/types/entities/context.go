package entities

import "time"

type Context struct {
	ID            string                 `json:"id" bson:"_id,omitempty"`
	Name          string                 `json:"name,omitempty" bson:"name,omitempty"`
	Description   string                 `json:"description,omitempty" bson:"description,omitempty"`
	Content       string                 `json:"content,omitempty" bson:"content,omitempty"`
	Organizations []OrganizationStub     `json:"organization,omitempty" bson:"organization,omitempty"`
	Tenants       []TenantStub           `json:"tenant,omitempty" bson:"tenant,omitempty"`
	Groups        []GroupStub            `json:"group,omitempty" bson:"group,omitempty"`
	User          UserStub               `json:"user,omitempty" bson:"user,omitempty"`
	CreatedTime   time.Time              `json:"createdTime,omitempty" bson:"createdTime,omitempty"`
	ModifiedTime  time.Time              `json:"modifiedTime,omitempty" bson:"modifiedTime,omitempty"`
	IsActive      bool                   `json:"isActive,omitempty" bson:"isActive,omitempty"`
	Version       int                    `json:"version,omitempty" bson:"version,omitempty"`
	Tags          []string               `json:"tags,omitempty" bson:"tags,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty" bson:"metadata,omitempty"`
}

type ContextHistory struct {
	ID            string                 `json:"id,omitempty" bson:"_id,omitempty"`
	ContextID     string                 `json:"contextId,omitempty" bson:"contextId,omitempty"`
	Name          string                 `json:"name,omitempty" bson:"name,omitempty"`
	Description   string                 `json:"description,omitempty" bson:"description,omitempty"`
	Content       string                 `json:"content,omitempty" bson:"content,omitempty"`
	Organizations []OrganizationStub     `json:"organization,omitempty" bson:"organization,omitempty"`
	Tenants       []TenantStub           `json:"tenant,omitempty" bson:"tenant,omitempty"`
	Groups        []GroupStub            `json:"group,omitempty" bson:"group,omitempty"`
	User          UserStub               `json:"user,omitempty" bson:"user,omitempty"`
	CreatedTime   time.Time              `json:"createdTime,omitempty" bson:"createdTime,omitempty"`
	IsActive      bool                   `json:"isActive,omitempty" bson:"isActive,omitempty"`
	Version       int                    `json:"version,omitempty" bson:"version,omitempty"`
	Tags          []string               `json:"tags,omitempty" bson:"tags,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty" bson:"metadata,omitempty"`
}
