package routers

import (
	"ai-transform-backend/transform-web-api/controllers"

	"github.com/gin-gonic/gin"
)

func InitTransformRouters(g *gin.RouterGroup, controller *controllers.Transform) {
	v1 := g.Group("/v1")
	v1.POST("/translate", controller.Translate)
	v1.GET("/records", controller.GetRecords)

	// 校对相关路由
	v1.GET("/proofread/pending", controller.GetPendingProofreads)
	v1.GET("/proofread/:id/srt", controller.GetProofreadSrt)
	v1.POST("/proofread/:id/submit", controller.SubmitProofread)

	// 死信队列（失败任务）相关路由
	v1.GET("/deadletter/failed", controller.GetFailedRecords)
	v1.POST("/deadletter/:id/retry", controller.RetryFailedRecord)
}
