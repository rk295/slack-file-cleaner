package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/slack-go/slack"
	"go.uber.org/zap"
)

const (
	daysOld     = 120 // Number of days to keep files for
	saveDir     = "files"
	tokenEnvVar = "TOKEN"
)

type server struct {
	log   *zap.SugaredLogger
	slack *slack.Client
}

func main() {

	s := &server{}

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	log := logger.Sugar()
	s.log = log

	slackToken := os.Getenv(tokenEnvVar)
	if slackToken == "" {
		s.log.Errorf("%s env var must be set", tokenEnvVar)
		os.Exit(1)
	}

	api := slack.New(slackToken)
	s.slack = api

	err := s.listFiles()
	if err != nil {
		s.log.Error(err)
		os.Exit(1)
	}
}

func (s *server) listFiles() error {

	now := time.Now()
	day := 24 * time.Hour
	oneMonth := now.Add(-daysOld * day)

	to := slack.JSONTime(oneMonth.Unix())

	params := slack.GetFilesParameters{
		Count:       1000,
		TimestampTo: to,
	}

	// lazzily ignoring pagination. It'll run every night, we don't upload 1000
	// a day so it will eventually catch up.
	files, _, err := s.slack.GetFiles(params)
	if err != nil {
		return err
	}

	fileCount := len(files)
	if fileCount == 0 {
		return nil
	}

	s.log.Infof("found %v files for deletion", fileCount)

	for _, file := range files {

		err := s.getFile(file)
		if err != nil {
			s.log.Infof("error saving file %s: %s", file.ID, err)
			// If we couldn't save if, don't delete it
			continue
		}

		err = s.slack.DeleteFile(file.ID)
		if err != nil {
			s.log.Infof("delete of %s failed: %s", file.ID, err)
		}
	}

	return nil
}

func (s *server) getFile(file slack.File) error {
	filename := fmt.Sprintf("%s-%s", file.ID, file.Name)
	year, month, day := file.Timestamp.Time().Date()
	datePath := fmt.Sprintf("%v/%02d/%v", year, month, day)

	dir := filepath.Join(saveDir, datePath)
	fullFilePath := filepath.Join(dir, filename)

	s.log.Debugf("id=%s name=%s Timestamp=%s fullFilePath=%s", file.ID, file.Name, file.Timestamp, fullFilePath)

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(fullFilePath)
	if err != nil {
		return err
	}

	err = s.slack.GetFile(file.URLPrivateDownload, f)
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	return nil

}
