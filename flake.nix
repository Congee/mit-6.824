{
  description = "Solutions for MIT 6.824 Distributed Systems Labs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = import nixpkgs { inherit system; };
      in
      {
        devShell = pkgs.mkShell {
          buildInputs = [
            pkgs.go
            pkgs.gotools
            pkgs.golangci-lint
            pkgs.gopls
            pkgs.go-outline
            pkgs.gopkgs
            pkgs.delve
            pkgs.impl

            pkgs.lnav
            pkgs.graphviz-nox

            pkgs.python310
            pkgs.python310Packages.rich
            pkgs.python310Packages.typer
            pkgs.python310Packages.ipdb
          ];
          shellHook = ''
            export GOROOT=$(go env GOROOT)
          '';
        };
      });
}

