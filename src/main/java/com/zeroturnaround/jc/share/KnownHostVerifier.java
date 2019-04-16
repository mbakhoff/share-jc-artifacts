package com.zeroturnaround.jc.share;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import com.trilead.ssh2.ServerHostKeyVerifier;

public class KnownHostVerifier implements ServerHostKeyVerifier {

  private List<KeyEntry> entries;

  @Override
  public boolean verifyServerHostKey(String hostname, int port, String serverHostKeyAlgorithm, byte[] serverHostKey) throws Exception {
    if (port != 22)
      return false;

    for (KeyEntry entry : getEntries()) {
      if (entry.matches(hostname, serverHostKeyAlgorithm, serverHostKey))
        return true;
    }
    return false;
  }

  private synchronized List<KeyEntry> getEntries() throws IOException {
    if (entries == null) {
      Path systemEntries = Paths.get("/etc/ssh/ssh_known_hosts");
      Path userEntries = Paths.get(System.getProperty("user.home"), ".ssh", "known_hosts");
      List<KeyEntry> list = new ArrayList<>();
      if (Files.exists(systemEntries))
        list.addAll(readEntries(systemEntries));
      if (Files.exists(userEntries))
        list.addAll(readEntries(userEntries));
      entries = list;
    }
    return entries;
  }

  private List<KeyEntry> readEntries(Path path) throws IOException {
    return Files.readAllLines(path).stream()
        .map(KeyEntry::new)
        .collect(Collectors.toList());
  }

  static class KeyEntry {
    final List<String> names;
    final String type;
    final String encodedKey;

    KeyEntry(String line) {
      String[] parts = line.split(" ");
      names = Arrays.asList(parts[0].split(","));
      type = parts[1];
      encodedKey = parts[2];
    }

    byte[] key() {
      return Base64.getDecoder().decode(encodedKey);
    }

    public boolean matches(String hostname, String serverHostKeyAlgorithm, byte[] serverHostKey) {
      return names.contains(hostname) && type.equals(serverHostKeyAlgorithm) && Arrays.equals(key(), serverHostKey);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " " + names;
    }
  }
}
