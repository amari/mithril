package directory

import "go.uber.org/fx"

func Module() fx.Option {
	return fx.Module("volume.directory",
		fx.Provide(NewDirectoryVolumeExpert),
	)
}
