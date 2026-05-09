package gobblerclient

import "context"

// nopClient is a no-op implementation of Client. All methods return nil.
type nopClient struct{}

// Nop returns a no-op Client. Safe for use anywhere; all methods are no-ops.
func Nop() Client {
	return &nopClient{}
}

func (n *nopClient) Log(typeName string, fields map[string]any) error {
	return nil
}

func (n *nopClient) Flush(ctx context.Context) error {
	return nil
}

func (n *nopClient) Close(ctx context.Context) error {
	return nil
}

func (n *nopClient) SwapServer(newURL string) error {
	return nil
}
