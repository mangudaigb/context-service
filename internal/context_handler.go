package internal

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
)

type ContextRequest struct {
	ID             string `json:"id"`
	OrganizationID string `json:"organization_id"`
	UserID         string `json:"user_id"`
}

type ContextHandler struct {
	log *logger.Logger
	svc ContextService
}

func NewContextHandler(log *logger.Logger, svc ContextService) *ContextHandler {
	return &ContextHandler{
		log: log,
		svc: svc,
	}
}

func (ch *ContextHandler) GetContextByFilter(c *gin.Context) {
	var reqData ContextRequest
	if err := c.ShouldBindJSON(&reqData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	list, err := ch.svc.FilterContexts(c.Request.Context(), reqData)
	if err != nil {
		ch.log.Errorf("Error filtering contexts: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	c.JSON(http.StatusOK, list)
}

func (ch *ContextHandler) GetContext(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID is required"})
		return
	}

	doc, err := ch.svc.GetContextByID(c.Request.Context(), id)
	if err != nil {
		ch.log.Errorf("Error getting context %s: %v", id, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if doc == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Context not found"})
		return
	}

	c.JSON(http.StatusOK, doc)
}

func (ch *ContextHandler) CreateContext(c *gin.Context) {
	var newContext entities.Context
	if err := c.ShouldBindJSON(&newContext); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	createdDoc, err := ch.svc.CreateContext(c.Request.Context(), &newContext)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create context: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, createdDoc)
}

func (ch *ContextHandler) UpdateContext(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID is required"})
		return
	}

	var updates entities.Context
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if updates.Version == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Optimistic lock failed: 'version' field is required in the update payload"})
		return
	}

	if updates.ID != id {
		ch.log.Errorf("ID mismatch in update payload: %s != %s", updates.ID, id)
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID mismatch in update payload"})
		return
	}

	updatedDoc, err := ch.svc.UpdateContext(c.Request.Context(), &updates)
	if err != nil {
		if errors.Is(err, errors.New("document update failed: document not found or version mismatch (Optimistic Lock failure)")) {
			c.JSON(http.StatusConflict, gin.H{"error": "Update failed due to version mismatch or document not found."})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update context: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, updatedDoc)
}

// DeleteContext deletes a document by ID.
func (ch *ContextHandler) DeleteContext(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID is required"})
		return
	}

	_, err := ch.svc.DeleteContext(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, errors.New("document not found for deletion")) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Context not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete context: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Context deleted successfully"})
}
