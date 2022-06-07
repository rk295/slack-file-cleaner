package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/slack-go/slack"
	"go.uber.org/zap"
)

const (
	daysOld     = 90 // Number of days to keep files for
	saveDir     = "files"
	tokenEnvVar = "TOKEN"
)

type server struct {
	log   *zap.SugaredLogger
	slack *slack.Client
}

func main() {

	s := &server{}

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	log := logger.Sugar()
	s.log = log

	s.log.Debug("starting")

	slackToken := os.Getenv(tokenEnvVar)
	if slackToken == "" {
		s.log.Errorf("%s env var must be set", tokenEnvVar)
		os.Exit(1)
	}

	api := slack.New(slackToken)
	s.slack = api

	ctx := context.Background()

	files, err := s.listFiles(ctx)
	if err != nil {
		s.log.Error(err)
		os.Exit(1)
	}

	err = s.processFiles(ctx, files)
	if err != nil {
		s.log.Error(err)
		os.Exit(1)
	}

}

func (s *server) listFiles(ctx context.Context) (files []slack.File, err error) {

	now := time.Now()
	day := 24 * time.Hour
	oneMonth := now.Add(-daysOld * day)

	to := slack.JSONTime(oneMonth.Unix())

	params := slack.GetFilesParameters{
		Count:       100,
		TimestampTo: to,
		ShowHidden:  true,
		Page:        1,
	}

	files, paging, err := s.slack.GetFiles(params)
	for err == nil {
		params.Page++
		if params.Page > paging.Pages {
			break
		}

		f, _, err := s.slack.GetFiles(params)
		if err == nil {
			files = append(files, f...)
		} else if rateLimitedError, ok := err.(*slack.RateLimitedError); ok {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(rateLimitedError.RetryAfter):
				err = nil
			}
		}
	}
	return files, err
}

func (s *server) processFiles(ctx context.Context, files []slack.File) error {
	fileCount := len(files)
	if fileCount == 0 {
		s.log.Debugf("found no files to delete that were older than %d days", daysOld)
		return nil
	}

	s.log.Debugf("found %v files for deletion", fileCount)

	for _, file := range files {

		if file.Mode == "hidden_by_limit" {
			s.log.Debugf("file id %s is hidden by free quota limit, won't download before deleting", file.ID)
		} else {
			err := s.getFile(file)
			if err != nil {
				s.log.Errorf("error saving file %s: %s", file.ID, err)
				continue
			}
		}

		err := s.deleteFile(ctx, file.ID)
		if err != nil {
			s.log.Error(err)
		}
	}

	return nil
}

func (s *server) deleteFile(ctx context.Context, fileID string) (err error) {
	for err == nil {
		err := s.slack.DeleteFile(fileID)
		if err == nil {
			return err
		} else if rateLimitedError, ok := err.(*slack.RateLimitedError); ok {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(rateLimitedError.RetryAfter):
				err = nil
			}
		}
	}
	return err
}

func (s *server) getFile(file slack.File) error {

	if file.URLPrivateDownload == "" {
		s.log.Warnf("URLPrivateDownload field for file %s is empty, skipping download", file.ID)
		return nil
	}

	filename := fmt.Sprintf("%s-%s", file.ID, file.Name)
	year, month, day := file.Timestamp.Time().Date()
	datePath := fmt.Sprintf("%v/%02d/%v", year, month, day)

	dir := filepath.Join(saveDir, datePath)
	fullFilePath := filepath.Join(dir, filename)

	s.log.Debugf("file_id=%s user_name=%s name=%s Timestamp=%s fullFilePath=%s", file.ID, s.getUser(file.User), file.Name, file.Timestamp, fullFilePath)

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

func (s *server) getUser(userID string) string {

	user, err := s.slack.GetUserInfo(userID)

	if err != nil {
		s.log.Warnf("error fetching user details for user_id=%s error:%s", userID, err)
		return "user-lookup-failed"
	}

	return user.Name
}
