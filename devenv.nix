{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  packages = [
    pkgs.git
    pkgs.bear
    pkgs.gcc
    pkgs.gnumake
    pkgs.cmake
  ];

  env = {
    CXX = "g++";
    CXXFLAGS = "-std=c++20 -Wall -Wextra -O2";
  };
}
