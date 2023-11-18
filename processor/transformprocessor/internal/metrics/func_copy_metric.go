// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor/internal/metrics"

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
)

type copyMetricArguments struct {
	newName string
}

func newCopyMetricFactory() ottl.Factory[ottlmetric.TransformContext] {
	return ottl.NewFactory("copy_metric", &copyMetricArguments{}, createCopyMetricFunction)
}

func createCopyMetricFunction(_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[ottlmetric.TransformContext], error) {
	args, ok := oArgs.(*copyMetricArguments)

	if !ok {
		return nil, fmt.Errorf("newCopyMetricFactory args must be of type *copyMetricArguments")
	}
	return copyMetric(args.newName)
}

func copyMetric(newName string) (ottl.ExprFunc[ottlmetric.TransformContext], error) {
	return func(_ context.Context, tCtx ottlmetric.TransformContext) (any, error) {
		metric := tCtx.GetMetric()

		newMetric := tCtx.GetMetrics().AppendEmpty()
		metric.CopyTo(newMetric)
		newMetric.SetName(newName)

		return nil, nil
	}, nil
}
