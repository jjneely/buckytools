package main

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
)

func TestIsTokenValid(t *testing.T) {
	authJWTSecretKey = []byte("1fPpsptbs4aTWxGEV9gTIU4xagl0W9XSbgGuFLTFXPlgZQ+24/jcrxKgan1b")

	for i, casex := range []struct {
		claim  jwt.MapClaims
		metric string
		op     ACLOperation
		valid  error
	}{
		{
			claim: jwt.MapClaims{
				"iat":        time.Now().Unix(),
				"namespaces": []string{"*"},
				"ops":        []string{"*"},
			},
			metric: "sys.test",
			op:     ACLDeleteMetrics,
			valid:  nil,
		},
		{
			claim: jwt.MapClaims{
				"iat":        time.Now().Unix(),
				"namespaces": []string{"*"},
				"ops":        []string{"read"},
			},
			metric: "sys.test",
			op:     ACLDeleteMetrics,
			valid:  errors.New("token doesn't have delete permission for the metric/namespace"),
		},
		{
			claim: jwt.MapClaims{
				"iat":        time.Now().Unix(),
				"namespaces": []string{"*"},
				"ops":        []string{"*"},
			},
			metric: "*",
			op:     ACLDeleteMetrics,
			valid:  nil,
		},
		{
			claim: jwt.MapClaims{
				"iat":        time.Now().Unix(),
				"namespaces": []string{"general.*"},
				"ops":        []string{"*"},
			},
			metric: "sys.test",
			op:     ACLDeleteMetrics,
			valid:  errors.New("token has no access to the metric/namespace"),
		},
		{
			claim: jwt.MapClaims{
				"iat":        time.Now().Unix(),
				"namespaces": []string{"general.*"},
				"ops":        []string{"*"},
			},
			metric: "general.test",
			op:     ACLDeleteMetrics,
			valid:  nil,
		},
		{
			claim: jwt.MapClaims{
				"iat":        time.Now().Unix(),
				"namespaces": []string{"general.*"},
				"opsx":       []string{"*"},
			},
			metric: "general.test",
			op:     ACLDeleteMetrics,
			valid:  errors.New("token doesn't have delete permission for the metric/namespace"),
		},
		{
			claim: jwt.MapClaims{
				"iat":         time.Now().Unix(),
				"namespacesx": []string{"general.*"},
				"ops":         []string{"*"},
			},
			metric: "general.test",
			op:     ACLDeleteMetrics,
			valid:  errors.New("token has no access to the metric/namespace"),
		},
	} {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			token := jwt.NewWithClaims(jwt.SigningMethodHS256, casex.claim)

			// Sign and get the complete encoded token as a string using the secret
			tokenString, err := token.SignedString(authJWTSecretKey)
			if err != nil {
				panic(err)
			}

			valid := isTokenValid(casex.metric, tokenString, casex.op)
			if got, want := valid, casex.valid; !reflect.DeepEqual(got, want) {
				t.Errorf("valid = %v; want %v", got, want)
			}
		})
	}

}
