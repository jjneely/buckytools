package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/golang-jwt/jwt"
)

type ACL struct {
	Namespaces []string
	Ops        []ACLOperation // post, put, delete
}

func (*ACL) Valid() error { return nil }

type ACLOperation string

const (
	ACLReadMetrics    ACLOperation = "read"
	ACLUpdateMetrics  ACLOperation = "update"
	ACLReplaceMetrics ACLOperation = "replace"
	ACLDeleteMetrics  ACLOperation = "delete"
)

// root acl example: {"namespaces": ["*"], "ops": ["*"]}
func isTokenValid(metric string, tokenStr string, op ACLOperation) error {
	if authJWTSecretKey == nil {
		return nil
	}

	token, err := jwt.ParseWithClaims(tokenStr, &ACL{}, func(*jwt.Token) (interface{}, error) { return authJWTSecretKey, nil })
	if err != nil {
		return err
	}

	acl, ok := token.Claims.(*ACL)
	if !ok {
		return errors.New("failed to retrieve acl")
	} else if !token.Valid {
		return errors.New("token not valid")
	}

	var hasAccess bool
	for _, ns := range acl.Namespaces {
		if m, err := filepath.Match(ns, metric); !m {
			continue
		} else if err != nil {
			return fmt.Errorf("broken namespaces in token %s: %w", ns, err)
		}
		hasAccess = true
		break
	}

	if !hasAccess {
		return errors.New("token has no access to the metric/namespace")
	}

	var canOperate bool
	for _, opx := range acl.Ops {
		if opx != "*" && opx != op {
			continue
		}
		canOperate = true
		break
	}

	if !canOperate {
		return fmt.Errorf("token doesn't have %s permission for the metric/namespace", op)
	}

	return nil
}

func generateRootAPITokenForInterBuckydAPICalls() (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"namespaces": []string{"*"},
		"ops":        []string{"*"},
	})

	return token.SignedString(authJWTSecretKey)
}
