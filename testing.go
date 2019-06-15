package testing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/pkg/errors"
)

// EnvFirebaseAPIKey is the environment variable holding the firebase API key
// used to login users.
const EnvFirebaseAPIKey = "FIREBASE_APIKEY"

// GoldenUpdate updates a golden file stored in the testdata directory based
// on the current t.Name() with the body argument.
func GoldenUpdate(t *testing.T, body []byte) {
	var goldenPath = fmt.Sprintf("testdata/%s", t.Name())
	if err := os.MkdirAll(goldenPath, 0777); err != nil {
		t.Fatalf("golden file directories could not be created and/or found: %s", err)
	}

	var goldenFile = fmt.Sprintf("%s/result.golden", goldenPath)
	if err := ioutil.WriteFile(goldenFile, body, 0644); err != nil {
		t.Fatal(errors.Wrap(err, "golden file could not be written"))
	}
}

// GoldenGet gets a specified golden file from the from the testdata
// directory based on the current t.Name().
func GoldenGet(t *testing.T) []byte {
	var goldenFile = fmt.Sprintf("testdata/%s/result.golden", t.Name())
	expected, err := ioutil.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(errors.Wrap(err, "failed to read golden file"))
	}
	return expected
}

// Authenticate authenticates a user with agnus-server via firebase.
func Authenticate(email, password string) string {
	reqBody, err := json.Marshal(
		struct {
			ReturnSecureToken bool   `json:"returnSecureToken"`
			Email             string `json:"email"`
			Password          string `json:"password"`
		}{
			ReturnSecureToken: true,
			Email:             email,
			Password:          password,
		},
	)
	if err != nil {
		panic(errors.Wrap(err, "failed to marshal authenticate request"))
	}

	resp, err := http.Post(
		fmt.Sprintf(
			"https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key=%s",
			os.Getenv(EnvFirebaseAPIKey)),
		"application/json",
		bytes.NewBuffer(reqBody),
	)
	if err != nil {
		panic(errors.Wrap(err, "failed to POST authentication"))
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(errors.Wrap(err, "failed to read response body"))
	}

	var f interface{}
	if err := json.Unmarshal(respBody, &f); err != nil {
		panic(errors.Wrap(err, "failed to unmarshal JSON response body"))
	}

	return f.(map[string]interface{})["idToken"].(string)
}
