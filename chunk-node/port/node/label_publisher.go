package portnode

type NodeLabelPublisher interface {
	PublishNodeLabels(labels map[string]string) error
}
