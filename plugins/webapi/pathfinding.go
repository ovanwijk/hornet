package webapi

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gohornet/hornet/pkg/model/tangle"
	"github.com/mitchellh/mapstructure"
)

func init() {
	addEndpoint("findPaths", getFindPaths, implementedAPIcalls)
}

func getFindPaths(i interface{}, c *gin.Context, abortSignal <-chan struct{}) {
	e := ErrorReturn{}
	query := &GetFindPath{}

	if err := mapstructure.Decode(i, query); err != nil {
		e.Error = fmt.Sprintf("%v: %v", ErrInternalError, err)
		c.JSON(http.StatusInternalServerError, e)
		return
	}

	cachedStartTx := tangle.GetCachedTransactionOrNil(query.Start) // tx +1
	if cachedStartTx == nil {
		e.Error = fmt.Sprintf("Start transaction not found: %v", query.Start)
		c.JSON(http.StatusBadRequest, e)
		return
	}

	c.JSON(http.StatusOK, query)
}
