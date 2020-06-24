package webapi

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gohornet/hornet/pkg/model/tangle"
	"github.com/gohornet/hornet/plugins/pathfinding"
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
	cachedStartTx.Release(false)

	for _, endpHash := range query.Endpoints {
		endp := tangle.GetCachedTransactionOrNil(endpHash) // tx +1
		if cachedStartTx == nil {
			e.Error = fmt.Sprintf("End transaction not found: %v", query.Start)
			c.JSON(http.StatusBadRequest, e)
			return
		}
		endp.Release(false)
	}
	transactions := make([]string, 0)
	branches := make([][]int, 0)
	trunks := make([][]int, 0)
	err := ""
	transactions, branches, trunks, err = pathfinding.FindPaths(query.Start, query.Endpoints)
	if err != "" {
		c.JSON(http.StatusBadGateway, err)
	}
	c.JSON(http.StatusOK, GetFindPathReturn{
		transactions,
		branches,
		trunks,
		0,
	})
}
