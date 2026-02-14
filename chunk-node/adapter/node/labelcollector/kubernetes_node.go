package adapternodelabeler

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubernetesNode struct {
	clientSet *kubernetes.Clientset
	nodeName  string
}

func NewKubernetesNodeLabelCollector(log *zerolog.Logger) (*KubernetesNode, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Warn().Err(err).Msg("not running in a Kubernetes cluster, skipping KubernetesNode label collector")

		return nil, nil
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	nodeName := getFirstEnv("NODE_NAME", "NODENAME")
	if nodeName == "" {
		return nil, errors.New("NODE_NAME environment variable is not set")
	}

	return &KubernetesNode{
		clientSet: clientset,
		nodeName:  nodeName,
	}, nil
}

func (k *KubernetesNode) CollectNodeLabels(ctx context.Context) (map[string]string, error) {
	if k == nil {
		return nil, nil
	}

	node, err := k.clientSet.CoreV1().Nodes().Get(ctx, k.nodeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsForbidden(err) {
			return nil, fmt.Errorf("%w\n\n"+
				"The pod's service account lacks permission to read node labels.\n"+
				"To fix this, create a ClusterRole with the \"get\" verb for nodes and bind it to the service account:\n\n"+
				"  apiVersion: rbac.authorization.k8s.io/v1\n"+
				"  kind: ClusterRole\n"+
				"  metadata:\n"+
				"    name: node-reader\n"+
				"  rules:\n"+
				"  - apiGroups: [\"\"]\n"+
				"    resources: [\"nodes\"]\n"+
				"    verbs: [\"get\"]\n"+
				"  ---\n"+
				"  apiVersion: rbac.authorization.k8s.io/v1\n"+
				"  kind: ClusterRoleBinding\n"+
				"  metadata:\n"+
				"    name: node-reader-binding\n"+
				"  subjects:\n"+
				"  - kind: ServiceAccount\n"+
				"    name: <service-account-name>\n"+
				"    namespace: <namespace>\n"+
				"  roleRef:\n"+
				"    kind: ClusterRole\n"+
				"    name: node-reader\n"+
				"    apiGroup: rbac.authorization.k8s.io", err)
		}
		return nil, err
	}

	return node.Labels, nil
}

func (k *KubernetesNode) Close() error {
	return nil
}
