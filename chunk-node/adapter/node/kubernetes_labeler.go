package node

import (
	"context"
	"errors"
	"maps"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/amari/mithril/chunk-node/port"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubernetesNodeLabeler struct {
	clientset *kubernetes.Clientset
	labels    map[string]string
	mu        sync.Mutex
	flag      atomic.Bool
}

var _ port.NodeLabeler = (*KubernetesNodeLabeler)(nil)

func KubernetesInClusterNodeLabeler() (port.NodeLabeler, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &KubernetesNodeLabeler{
		clientset: clientset,
	}, nil
}

func (nl *KubernetesNodeLabeler) Labels(ctx context.Context) (map[string]string, error) {
	podName := getFirstEnv("POD_NAME", "PODNAME", "HOSTNAME")
	if podName == "" {
		return nil, errors.New("POD_NAME environment variable is not set")
	}

	podNamespaceBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return nil, err
	}
	podNamespace := strings.TrimSpace(string(podNamespaceBytes))

	nodeName := getFirstEnv("NODE_NAME", "NODENAME")
	if nodeName == "" {
		return nil, errors.New("NODE_NAME environment variable is not set")
	}

	labels := map[string]string{}

	node, err := nl.clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	maps.Copy(labels, node.Labels)

	pod, err := nl.clientset.CoreV1().Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	maps.Copy(labels, pod.Labels)

	return labels, nil
}

func getFirstEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}
