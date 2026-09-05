package gist

import (
	"context"
	"github.com/joho/godotenv"
	"log"
	"os"
)

type Option func(server *Server) error

type App struct {
	ctx     context.Context
	options []Option
}

func NewApp(ctx context.Context, options ...Option) *App {
	_ = godotenv.Load(".env")
	return &App{ctx: ctx, options: options}
}

func (a *App) Run() {
	server, err := Start(Config{
		ConfigPath: envOrDefault("CONFIG_FILE", "./config/config.json"),
	})
	if err != nil {
		log.Fatalf("gistsdk: could not start gist-server: %v", err)
	}
	defer func(server *Server) {
		_ = server.Stop()
	}(server)

	for _, opt := range a.options {
		if err := opt(server); err != nil {
			if err := server.Stop(); err != nil {
				return
			}
			server.Logger.Panic("gistsdk: could not apply option", map[string]any{"error": err})
		}
	}

	if err := server.ValidateMiddlewares(); err != nil {
		if stopErr := server.Stop(); stopErr != nil {
			return
		}
		server.Logger.Panic("gistsdk: middleware validation failed", map[string]any{"error": err})
	}

	server.WaitForInterrupt()
}

func envOrDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
