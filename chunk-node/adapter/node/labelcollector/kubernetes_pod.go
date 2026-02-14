package adapternodelabeler

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/rs/zerolog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubernetesPod struct {
	clientSet    *kubernetes.Clientset
	podName      string
	podNamespace string
}

func NewKubernetesPodLabelCollector(log *zerolog.Logger) (*KubernetesPod, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Warn().Err(err).Msg("not running in a Kubernetes cluster, skipping KubernetesPod label collector")

		return nil, nil
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	podName := getFirstEnv("POD_NAME", "PODNAME", "HOSTNAME")
	if podName == "" {
		return nil, errors.New("POD_NAME environment variable is not set")
	}

	podNamespaceBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return nil, err
	}
	podNamespace := strings.TrimSpace(string(podNamespaceBytes))

	return &KubernetesPod{
		clientSet:    clientset,
		podName:      podName,
		podNamespace: podNamespace,
	}, nil
}

func (k *KubernetesPod) CollectNodeLabels(ctx context.Context) (map[string]string, error) {
	if k == nil {
		return nil, nil
	}

	pod, err := k.clientSet.CoreV1().Pods(k.podNamespace).Get(ctx, k.podName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return pod.Labels, nil
}

func (k *KubernetesPod) Close() error {
	return nil
}

func getFirstEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}
