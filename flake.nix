{
  description =
    "tmducken - tech.ml.dataset Integration with DuckDB for Clojure";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    clj-nix.url = "github:jlesquembre/clj-nix";
  };

  outputs = { self, nixpkgs, flake-utils, clj-nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        cljpkgs = clj-nix.packages.${system};
        # Simulating JNA Platform#getNativeLibraryResourcePrefix()
        # Flip arch-os from Nix to os-arch
        system-parts = pkgs.lib.splitString "-" pkgs.system;
        os-arch-prefix-underscores =
          builtins.concatStringsSep "-" (pkgs.lib.reverseList system-parts);
        # NOTE: ^ this contains x86_64, have to fix to be equivalent to JNA
        os-arch-prefix =
          builtins.replaceStrings [ "_" ] [ "-" ] os-arch-prefix-underscores;
        lib-folder = "./lib/${os-arch-prefix}";
        # Definining a reusable DLL providing script, used for Nix repeatable build AND direnv shell entering (development)
        postPatch = ''
          mkdir -p ${lib-folder}
          ln -sf ${pkgs.duckdb}/lib/* ${lib-folder}
        '';
      in {

        packages = {
          duckdb = pkgs.duckdb;
          default = let version = pkgs.duckdb.version;
          in cljpkgs.mkCljLib {
            inherit postPatch version;
            projectSrc = ./.;
            name = "com.techascent/tmducken";
            buildCommand = "clj -T:build jar --report stderr";
            preBuild = "export DUCKDB_VERSION=${version}";
          };
        };
        devShells.default = pkgs.mkShell {
          buildInputs = [
            # provides deps-lock library for updating deps-lock.json from deps.edn
            clj-nix.packages.${system}.deps-lock
            pkgs.clojure
          ];
          shellHook = postPatch;
        };
      });
}
