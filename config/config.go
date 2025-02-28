package config

import (
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the entire configuration for trino-cli.
type Config struct {
	Profiles map[string]Profile `yaml:"profiles"`
	Defaults Defaults           `yaml:"defaults"`
}

// Profile defines connection settings for a Trino profile.
type Profile struct {
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	User    string `yaml:"user"`
	Catalog string `yaml:"catalog"`
	Schema  string `yaml:"schema"`
}

// Defaults defines query defaults.
type Defaults struct {
	MaxRows int    `yaml:"max_rows"`
	Format  string `yaml:"format"`
}

// AppConfig is the global configuration instance.
var AppConfig Config

// LoadConfig reads configuration from a YAML file.
func LoadConfig(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("config file does not exist: %s", path)
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, &AppConfig)
}
