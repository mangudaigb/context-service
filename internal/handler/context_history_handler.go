package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	svc2 "github.com/mangudaigb/context-service/internal/svc"
	"github.com/mangudaigb/dhauli-base/logger"
)

type ContextHistoryHandler struct {
	log *logger.Logger
	svc svc2.ContextHistoryService
}

func NewContextHistoryHandler(log *logger.Logger, svc svc2.ContextHistoryService) *ContextHistoryHandler {
	return &ContextHistoryHandler{
		log: log,
		svc: svc,
	}
}

func (chh *ContextHistoryHandler) GetContextHistoryItem(c *gin.Context) {
	contextId := c.Param("cid")
	historyId := c.Param("hid")
	if contextId == "" && historyId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Context ID and Context History ID is required"})
		return
	}

	doc, err := chh.svc.GetContextHistoryByID(c.Request.Context(), contextId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Context History Service error"})
	}

	if doc == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Context History not found"})
	}
	c.JSON(http.StatusOK, doc)
}

func (chh *ContextHistoryHandler) GetContextHistoryForContextID(c *gin.Context) {
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
