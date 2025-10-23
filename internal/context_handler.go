package internal

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/mangudaigb/context-service/db"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
	"go.mongodb.org/mongo-driver/bson"
)

type ContextHandler struct {
	log  *logger.Logger
	Repo db.ContextRepository
}

func NewContextHandler(log *logger.Logger, repo db.ContextRepository) *ContextHandler {
	return &ContextHandler{
		log:  log,
		Repo: repo,
	}
}

func (h *ContextHandler) GetContext(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID is required"})
		return
	}

	doc, err := h.Repo.GetContextByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if doc == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Context not found"})
		return
	}

	c.JSON(http.StatusOK, doc)
}

func (h *ContextHandler) CreateContext(c *gin.Context) {
	var newContext entities.Context
	if err := c.ShouldBindJSON(&newContext); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	createdDoc, err := h.Repo.CreateContext(c.Request.Context(), &newContext)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create context: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, createdDoc)
}

func (h *ContextHandler) UpdateContext(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID is required"})
		return
	}

	// Use bson.M for unmarshaling to match the repository signature
	var updates bson.M
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Important: The client MUST provide the "version" for optimistic locking.
	if _, ok := updates["version"]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Optimistic lock failed: 'version' field is required in the update payload"})
		return
	}

	updatedDoc, err := h.Repo.UpdateContext(c.Request.Context(), id, updates)
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
func (h *ContextHandler) DeleteContext(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID is required"})
		return
	}

	err := h.Repo.DeleteContext(c.Request.Context(), id)
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

func SetupRouter(log *logger.Logger, repo db.ContextRepository) *gin.Engine {
	r := gin.Default()
	handler := NewContextHandler(log, repo)

	contextRoutes := r.Group("/contexts")
	{
		contextRoutes.GET("/:id", handler.GetContext)
		contextRoutes.POST("/", handler.CreateContext)
		contextRoutes.PATCH("/:id", handler.UpdateContext) // Using PATCH for partial updates
		contextRoutes.DELETE("/:id", handler.DeleteContext)
	}
	return r
}
