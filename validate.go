package gobblerclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// validateServer checks that serverURL points at a running Gobbler instance that
// has all of the required type names registered.
//
// It makes two GET requests:
//  1. GET <serverURL>/gobbler/pipeline/status — must return {"running": true, ...}
//  2. GET <serverURL>/gobbler/definition/names — must contain every name in types
//
// The provided httpClient is used for both requests so tests can inject a
// custom transport without affecting http.DefaultClient.
func validateServer(serverURL string, types map[string]struct{}, httpClient *http.Client) error {
	// --- 1. Check pipeline is running ---
	statusURL := serverURL + "/gobbler/pipeline/status"
	resp, err := httpClient.Get(statusURL)
	if err != nil {
		return fmt.Errorf("gobblerclient: validate: GET %s: %w", statusURL, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gobblerclient: validate: GET %s returned %d", statusURL, resp.StatusCode)
	}

	var status struct {
		Running bool `json:"running"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		return fmt.Errorf("gobblerclient: validate: parse status response: %w", err)
	}
	if !status.Running {
		return fmt.Errorf("gobblerclient: validate: pipeline is not running at %s", serverURL)
	}

	// Nothing to check if no types were registered.
	if len(types) == 0 {
		return nil
	}

	// --- 2. Check all registered types are present ---
	namesURL := serverURL + "/gobbler/definition/names"
	resp2, err := httpClient.Get(namesURL)
	if err != nil {
		return fmt.Errorf("gobblerclient: validate: GET %s: %w", namesURL, err)
	}
	defer resp2.Body.Close()
	body2, _ := io.ReadAll(resp2.Body)

	if resp2.StatusCode != http.StatusOK {
		return fmt.Errorf("gobblerclient: validate: GET %s returned %d", namesURL, resp2.StatusCode)
	}

	var names []string
	if err := json.Unmarshal(body2, &names); err != nil {
		return fmt.Errorf("gobblerclient: validate: parse names response: %w", err)
	}

	serverTypes := make(map[string]struct{}, len(names))
	for _, n := range names {
		serverTypes[n] = struct{}{}
	}

	var missing []string
	for t := range types {
		if _, ok := serverTypes[t]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("gobblerclient: validate: type(s) not registered on server: %v", missing)
	}

	return nil
}
