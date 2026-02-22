package adapternode

import (
	"context"
	"errors"
	"maps"
	"os"
	"strings"
	"sync"
	"time"

	portnode "github.com/amari/mithril/chunk-node/port/node"
	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubernetesLabelProvider struct {
	clientSet    *kubernetes.Clientset
	nodeName     string
	podName      string
	podNamespace string
	logger       *zerolog.Logger

	mu          sync.RWMutex
	nodeLabels  map[string]string
	podLabels   map[string]string
	subscribers map[chan struct{}]struct{}

	syncContext    context.Context
	syncCancelFunc context.CancelFunc
	syncWg         sync.WaitGroup
}

var _ portnode.NodeLabelProvider = (*KubernetesLabelProvider)(nil)

func NewKubernetesLabelProvider(log *zerolog.Logger) (*KubernetesLabelProvider, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Warn().Err(err).Msg("skipping KubernetesLabelProvider")

		return nil, nil
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	nodeName := getAnyEnv("NODE_NAME", "NODENAME")
	if nodeName == "" {
		err := errors.New("NODE_NAME environment variable is not set")

		log.Warn().Err(err).Msg("skipping KubernetesLabelProvider")

		return nil, nil
	}

	podName := getAnyEnv("POD_NAME", "PODNAME", "HOSTNAME")
	if podName == "" {
		err := errors.New("POD_NAME environment variable is not set")

		log.Warn().Err(err).Msg("skipping KubernetesLabelProvider")

		return nil, nil
	}

	podNamespaceBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		log.Warn().Err(err).Msg("skipping KubernetesLabelProvider")

		return nil, nil
	}
	podNamespace := strings.TrimSpace(string(podNamespaceBytes))

	return &KubernetesLabelProvider{
		clientSet:    clientset,
		nodeName:     nodeName,
		podName:      podName,
		podNamespace: podNamespace,
		logger:       log,
	}, nil
}

func (k *KubernetesLabelProvider) Start() error {
	if k == nil {
		return nil
	}

	syncCtx, cancelFunc := context.WithCancel(context.Background())
	k.syncContext = syncCtx
	k.syncCancelFunc = cancelFunc

	k.syncWg.Go(func() {
		k.syncNodeLabels(syncCtx)
	})
	k.syncWg.Go(func() {
		k.syncPodLabels(syncCtx)
	})

	return nil
}

func (k *KubernetesLabelProvider) Stop() error {
	if k == nil {
		return nil
	}

	k.syncCancelFunc()
	k.syncWg.Wait()

	return nil
}

func (k *KubernetesLabelProvider) GetNodeLabels() map[string]string {
	if k == nil {
		return nil
	}

	k.mu.RLock()
	defer k.mu.RUnlock()

	labels := make(map[string]string)

	maps.Copy(labels, k.nodeLabels)
	maps.Copy(labels, k.podLabels)

	return labels
}

func (k *KubernetesLabelProvider) Watch(watchCtx context.Context) <-chan struct{} {
	ch := make(chan struct{}, 1)

	if k == nil {
		close(ch)
		return ch
	}

	k.mu.Lock()
	if k.subscribers == nil {
		k.subscribers = make(map[chan struct{}]struct{})
	}
	k.subscribers[ch] = struct{}{}
	k.mu.Unlock()

	go func() {
		select {
		case <-watchCtx.Done():
		case <-k.syncContext.Done():
		}

		k.mu.Lock()
		delete(k.subscribers, ch)
		k.mu.Unlock()
		close(ch)
	}()

	return ch
}

func (k *KubernetesLabelProvider) notifySubscribers() {
	k.mu.RLock()
	defer k.mu.RUnlock()
	for ch := range k.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (k *KubernetesLabelProvider) syncNodeLabels(ctx context.Context) {
	bo := backoff.NewExponentialBackOff()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			node, err := k.clientSet.CoreV1().Nodes().Get(ctx, k.nodeName, metav1.GetOptions{})
			if err != nil {
				k.logger.Error().Err(err).Msg("failed to get node labels")

				select {
				case <-ctx.Done():
					return
				case <-time.After(bo.NextBackOff()):
				}

				continue
			}
			bo.Reset()

			k.mu.Lock()
			k.nodeLabels = node.Labels
			k.mu.Unlock()

			k.notifySubscribers()

			return
		}
	}
}

func (k *KubernetesLabelProvider) syncPodLabels(ctx context.Context) {
	bo := backoff.NewExponentialBackOff()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			pod, err := k.clientSet.CoreV1().Pods(k.podNamespace).Get(ctx, k.podName, metav1.GetOptions{})
			if err != nil {
				k.logger.Error().Err(err).Msg("failed to get pod labels")

				select {
				case <-ctx.Done():
					return
				case <-time.After(bo.NextBackOff()):
				}

				continue
			}
			bo.Reset()

			k.mu.Lock()
			k.podLabels = pod.Labels
			k.mu.Unlock()

			k.notifySubscribers()

			return
		}
	}
}

func getAnyEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}
