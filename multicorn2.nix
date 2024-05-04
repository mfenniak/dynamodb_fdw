{
  stdenv
  , fetchFromGitHub
  , postgresql
  , postgresqlTestHook
  , python3
  , python3Packages
}:

let
  targetVersion = "v2.5";

  multicornSrc = fetchFromGitHub {
    owner = "pgsql-io";
    repo = "multicorn2";
    rev = targetVersion;
    sha256 = "sha256-4fJ79zZIJbpTya/px4FG3tWnedQF5/0hlaJX+6BWcls=";
  };

  multicorn = stdenv.mkDerivation rec {
    pname = "multicorn2";
    version = targetVersion;
    src = multicornSrc;
    buildInputs = postgresql.buildInputs ++ [ postgresql python3 ];
    installPhase = ''
      runHook preInstall
      install -D multicorn${postgresql.dlSuffix} -t $out/lib/
      install -D sql/multicorn--''${version#v}.sql -t $out/share/postgresql/extension
      install -D multicorn.control -t $out/share/postgresql/extension
      runHook postInstall
    '';
  };

  multicornTest = stdenv.mkDerivation {
    name = "multicorn2-test";
    dontUnpack = true;
    doCheck = true;
    buildInputs = [ postgresqlTestHook ];
    nativeCheckInputs = [ (postgresql.withPackages (ps: [ multicorn ])) ];
    postgresqlTestUserOptions = "LOGIN SUPERUSER";
    failureHook = "postgresqlStop";
    checkPhase = ''
      runHook preCheck
      psql -a -v ON_ERROR_STOP=1 -c "CREATE EXTENSION multicorn;"
      runHook postCheck
    '';
    installPhase = "touch $out";
  };

  multicornPython = python3Packages.buildPythonPackage rec {
    pname = "multicorn2-python";
    version = targetVersion;
    src = multicornSrc;
    nativeBuildInputs = [ postgresql ];
  };

  multicornPythonTest = stdenv.mkDerivation {
    name = "multicorn2-python-test";
    dontUnpack = true;
    doCheck = true;
    buildInputs = [ postgresqlTestHook ];
    nativeCheckInputs = [
      (postgresql.withPackages (ps: [ multicorn ]))
      (python3.withPackages (ps: [ multicornPython ]))
    ];
    postgresqlTestUserOptions = "LOGIN SUPERUSER";
    failureHook = "postgresqlStop";
    checkPhase = ''
      runHook preCheck

      # extracted from multicorn_logger_test.sql
      psql -a -v ON_ERROR_STOP=1 -c "CREATE EXTENSION multicorn;"
      psql -a -v ON_ERROR_STOP=1 -c "CREATE server multicorn_srv foreign data wrapper multicorn options (
          wrapper 'multicorn.testfdw.TestForeignDataWrapper'
      );"
      psql -a -v ON_ERROR_STOP=1 -c "CREATE foreign table testmulticorn (
          test1 character varying,
          test2 character varying
      ) server multicorn_srv options (
          option1 'option1',
          test_type 'logger'
      );"
      psql -a -v ON_ERROR_STOP=1 -c "select * from testmulticorn;"
      psql -a -v ON_ERROR_STOP=1 -c "DROP EXTENSION multicorn cascade;"

      runHook postCheck
    '';
    installPhase = "touch $out";
  };

in {
  # nix-build -E 'with import <nixpkgs> {}; callPackage ./multicorn2.nix {}' -A package -A pythonPackage
  package = multicorn;
  pythonPackage = multicornPython;

  # nix-build -E 'with import <nixpkgs> {}; callPackage ./multicorn2.nix {}' -A tests.extension -A tests.python
  tests = {
    extension = multicornTest;
    python = multicornPythonTest;
  };
}
