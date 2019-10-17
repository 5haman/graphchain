package main

import (
	"io/ioutil"

	log "github.com/albrow/prtty"
  "github.com/go-redis/redis"
)

type Request struct {
	In  []string `json:"in"`
	Out []string `json:"out"`
}

type Response struct {
	Pairs []string `json:"pairs"`
	Edges []Edge   `json:"edges"`
}

type Edge struct {
	ID      string `json:"id"`
	Address string `json:"addr"`
}

func ScriptLoad(c *redis.Client) string {
	script, err := ioutil.ReadFile("script.lua")
	if err != nil {
		log.Error.Fatal(err)
	}
	sha, err := c.ScriptLoad(string(script)).Result()
	if err != nil {
		log.Error.Fatal(err)
	}
	return sha
}
