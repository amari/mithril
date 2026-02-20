package node

type NodeLabelPublisher interface {
	PublishNodeLabels(labels map[string]string) error
}
