package internal

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/mangudaigb/dhauli-base/logger"
)

type ContextHistoryHandler struct {
	log *logger.Logger
	svc ContextHistoryService
}

func NewContextHistoryHandler(log *logger.Logger, svc ContextHistoryService) *ContextHistoryHandler {
	return &ContextHistoryHandler{
		log: log,
		svc: svc,
	}
}

func (chh *ContextHistoryHandler) GetContextHistory(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context History ID is required"})
		return
	}

	doc, err := chh.svc.GetContextHistoryByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Context History Service error"})
	}

	if doc == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Context History not found"})
	}
	c.JSON(http.StatusOK, doc)
}

func (chh *ContextHistoryHandler) GetContextHistoryByContextID(c *gin.Context) {
	query, ok := c.GetQuery("cid")
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID is required"})
		return
	}
	doc, err := chh.svc.GetHistoryForContextId(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Context History Service error"})
	}
	c.JSON(http.StatusOK, doc)
}
