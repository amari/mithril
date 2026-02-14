package adapternodelabeler

import (
	"go.uber.org/fx"
)

func Module(labels map[string]string) fx.Option {
	return fx.Module("adapter.node.labeler",
		fx.Supply(&Runtime{}),
		fx.Provide(
			NewKubernetesNodeLabelCollector,
			NewKubernetesPodLabelCollector,
		),
	)
}
