package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver/factory"

	// fs drivers
	_ "github.com/docker/distribution/registry/storage/driver/azure"
	_ "github.com/docker/distribution/registry/storage/driver/filesystem"
	_ "github.com/docker/distribution/registry/storage/driver/inmemory"
	_ "github.com/docker/distribution/registry/storage/driver/middleware/cloudfront"
	_ "github.com/docker/distribution/registry/storage/driver/oss"
	_ "github.com/docker/distribution/registry/storage/driver/s3"
	_ "github.com/docker/distribution/registry/storage/driver/swift"
)

const (
	maxRepos = 500000
)

// based on https://github.com/docker/distribution/pull/867
func checkManifest(repoName string, mnfst *schema1.SignedManifest) error {
	if len(mnfst.FSLayers) == 0 || len(mnfst.History) == 0 {
		fmt.Printf("%s: no layers present\n", repoName)
	}

	if len(mnfst.FSLayers) != len(mnfst.History) {
		fmt.Printf("%s: mismatched layers and history\n", repoName)
	}

	// image provides a local type for validating the image relationship.
	type image struct {
		ID     string `json:"id"`
		Parent string `json:"parent"`
	}

	// Process the history portion to ensure that the parent links are
	// correctly represented. We serialize the image json, then walk the
	// entries, checking the parent link.
	var images []image
	for _, entry := range mnfst.History {
		var im image
		if err := json.Unmarshal([]byte(entry.V1Compatibility), &im); err != nil {
			fmt.Printf("%s: json unmarshal error: %v\n", repoName, err)
		}

		images = append(images, im)
	}

	// go through each image, checking the parent link and rank
	for i, image := range images {
		// ensure that the parent id is found in one of the subsequent
		// entries.
		if image.Parent != "" {
			id := ""
			for _, parentCandidate := range images[i:] {
				id = parentCandidate.ID
				if image.Parent == id {
					break
				}
			}
			if image.Parent != id {
				fmt.Printf("%s: parent not below in manifest (parent ID %v)\n", repoName, images[i].Parent)
			}
		}
	}

	return nil
}

func checkRepo(registry distribution.Namespace, repoName string) error {
	ctx := context.Background()

	repo, err := registry.Repository(ctx, repoName)
	if err != nil {
		return fmt.Errorf("unexpected error getting repository: %v", err)
	}
	manifests, err := repo.Manifests(ctx)
	if err != nil {
		return fmt.Errorf("unexpected error getting manifests: %v", err)
	}

	tags, err := manifests.Tags()
	if err != nil {
		return fmt.Errorf("unexpected error getting tags: %v", err)
	}

	fmt.Fprintf(os.Stderr, "checking repo %s (%d tags)\n", repoName)

	for _, tag := range tags {
		mnfst, err := manifests.GetByTag(tag)
		if err != nil {
			return fmt.Errorf("unexpected error getting manifest by tag: %v", err)
		}

		if err = checkManifest(repoName, mnfst); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	var configPath, reposPath string
	flag.StringVar(&configPath, "config", "", "path to a config file")
	flag.StringVar(&reposPath, "repos", "", "file with a list of repos")
	flag.Parse()

	if configPath == "" {
		fmt.Fprintln(os.Stderr, "must supply a config file with -config")
		flag.Usage()
		return
	}

	// Parse config file
	configFile, err := os.Open(configPath)
	if err != nil {
		panic(fmt.Sprintf("error opening config file: %v", err))
	}
	defer configFile.Close()

	config, err := configuration.Parse(configFile)
	if err != nil {
		panic(fmt.Sprintf("error parsing config file: %v", err))
	}

	ctx := context.Background()

	driver, err := factory.Create(config.Storage.Type(), config.Storage.Parameters())
	if err != nil {
		panic(fmt.Sprintf("error creating storage driver: %v", err))
	}

	registry, _ := storage.NewRegistry(ctx, driver, storage.BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()))

	var repos []string

	if reposPath != "" {
		reposFile, err := os.Open(reposPath)
		if err != nil {
			panic(fmt.Sprintf("could not open repos file: %v", err))
		}

		scanner := bufio.NewScanner(reposFile)
		for scanner.Scan() {
			repoName := scanner.Text()
			if len(repoName) > 0 {
				if repoName[0] == '+' {
					repoName = repoName[1:]
				}
				repos = append(repos, repoName)
			}
		}
	} else {
		repos = make([]string, maxRepos)

		n, err := registry.Repositories(ctx, repos, "")
		if err != nil && err != io.EOF {
			panic(fmt.Sprintf("unexpected error getting repo: %v", err))
		}
		if n == maxRepos {
			panic("too many repositories")
		}

		repos = repos[:n]
	}

	var wg sync.WaitGroup
	repoChan := make(chan string)

	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			for repoName := range repoChan {
				if err := checkRepo(registry, repoName); err != nil {
					fmt.Fprintln(os.Stderr, err)
				}
			}
			wg.Done()
		}()
	}

	for _, repoName := range repos {
		repoChan <- repoName
	}

	close(repoChan)

	wg.Wait()
}
