package gtcpresto

import "time"

const (
	version       = "1.0"
	userHeader    = "X-Presto-User"
	sourceHeader  = "X-Presto-Source"
	catalogHeader = "X-Presto-Catalog"
	schemaHeader  = "X-Presto-Schema"
	userAgent     = "tu-gc-presto/1.0"
	prestoSchema  = "default"
	prestoUser    = "zatxm"
	stateInit     = "NONE"
	initialRetry  = 50 * time.Millisecond
	maxRetry      = 800 * time.Millisecond
)
