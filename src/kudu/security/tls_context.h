// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/security/cert.h" // IWYU pragma: keep
#include "kudu/util/locks.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

class PrivateKey;
class TlsHandshake;

// TlsContext wraps data required by the OpenSSL library for creating and
// accepting TLS protected channels. A single TlsContext instance should be used
// per server or client instance.
//
// Internally, a 'TlsContext' manages a single keypair which it uses for
// terminating TLS connections. It also manages a collection of trusted root CA
// certificates (a trust store), as well as a signed certificate for the
// keypair.
//
// When used on a server, the TlsContext can generate a keypair and a
// self-signed certificate, and provide a CSR for transititioning to a CA-signed
// certificate. This allows Kudu servers to start with a self-signed
// certificate, and later adopt a CA-signed certificate as it becomes available.
// See GenerateSelfSignedCertAndKey(), GetCsrIfNecessary(), and
// AdoptSignedCert() for details on how to generate the keypair and self-signed
// cert, access the CSR, and transtition to a CA-signed cert, repectively.
//
// When used in a client or a server, the TlsContext can immediately adopt a
// private key and CA-signed cert using UseCertificateAndKey(). A TlsContext
// only manages a single keypair, so if UseCertificateAndKey() is called,
// GenerateSelfSignedCertAndKey() must not be called, and vice versa.
//
// TlsContext may be used with or without a keypair and cert to initiate TLS
// connections, when mutual TLS authentication is not needed (for example, for
// token or Kerberos authenticated connections).
//
// This class is thread-safe after initialization.
class TlsContext {

 public:

  TlsContext();

  // Create TLS context using the specified parameters:
  //  * tls_ciphers
  //      cipher suites preference list for TLSv1.2 and prior versions
  //  * tls_ciphersuites
  //      cipher suites preference list for TLSv1.3
  //  * tls_min_protocol
  //      minimum TLS protocol version to enable
  //  * tls_excluded_protocols
  //      TLS protocol versions to exclude from the list of enabled ones
  TlsContext(std::string tls_ciphers,
             std::string tls_ciphersuites,
             std::string tls_min_protocol,
             std::vector<std::string> tls_excluded_protocols = {});

  ~TlsContext() = default;

  Status Init() WARN_UNUSED_RESULT;

  // Returns true if this TlsContext has been configured with a cert and key for
  // use with TLS-encrypted connections.
  bool has_cert() const {
    shared_lock<RWMutex> lock(lock_);
    return has_cert_;
  }

  // Returns true if this TlsContext has been configured with a CA-signed TLS
  // cert and key for use with TLS-encrypted connections. If this method returns
  // true, then 'has_trusted_cert' will also return true.
  bool has_signed_cert() const {
    shared_lock<RWMutex> lock(lock_);
    return has_cert_ && !csr_;
  }

  // Returns true if this TlsContext has at least one certificate in its trust store.
  bool has_trusted_cert() const {
    shared_lock<RWMutex> lock(lock_);
    return trusted_cert_count_ > 0;
  }

  // Adds 'cert' as a trusted root CA certificate.
  //
  // This determines whether other peers are trusted. It also must be called for
  // any CA certificates that are part of the certificate chain for the cert
  // passed in to 'UseCertificateAndKey()' or 'AdoptSignedCert()'.
  //
  // If this cert has already been marked as trusted, this has no effect.
  Status AddTrustedCertificate(const Cert& cert) WARN_UNUSED_RESULT;

  // Dump all of the certs that are currently trusted by this context, in DER
  // form, into 'cert_ders'.
  Status DumpTrustedCerts(std::vector<std::string>* cert_ders) const WARN_UNUSED_RESULT;

  // Uses 'cert' and 'key' as the cert and key for use with TLS connections.
  //
  // Checks that the CA that issued the signature on 'cert' is already trusted
  // by this context (e.g. by AddTrustedCertificate()).
  Status UseCertificateAndKey(const Cert& cert, const PrivateKey& key) WARN_UNUSED_RESULT;

  // Generates a self-signed cert and key for use with TLS connections.
  //
  // This method should only be used on the server. Once this method is called,
  // 'GetCsrIfNecessary' can be used to retrieve a CSR for generating a
  // CA-signed cert for the generated private key, and 'AdoptSignedCert' can be
  // used to transition to using the CA-signed cert with subsequent TLS
  // connections.
  Status GenerateSelfSignedCertAndKey() WARN_UNUSED_RESULT;

  // Returns a new certificate signing request (CSR) in DER format, if this
  // context's cert is self-signed. If the cert is already signed, returns
  // std::nullopt.
  std::optional<CertSignRequest> GetCsrIfNecessary() const;

  // Adopts the provided CA-signed certificate for this TLS context.
  //
  // The certificate must correspond to a CSR previously returned by
  // 'GetCsrIfNecessary()'.
  //
  // Checks that the CA that issued the signature on 'cert' is already trusted
  // by this context (e.g. by AddTrustedCertificate()).
  //
  // This has no effect if the instance already has a CA-signed cert.
  Status AdoptSignedCert(const Cert& cert) WARN_UNUSED_RESULT;

  // Convenience functions for loading cert/CA/key from file paths.
  // -------------------------------------------------------------

  // Load the server certificate and key (PEM encoded).
  Status LoadCertificateAndKey(const std::string& certificate_path,
                               const std::string& key_path) WARN_UNUSED_RESULT;

  // Load the server certificate and key (PEM encoded), and use the callback
  // 'password_cb' to obtain the password that can decrypt the key.
  Status LoadCertificateAndPasswordProtectedKey(const std::string& certificate_path,
                                                const std::string& key_path,
                                                const PasswordCallback& password_cb)
                                                WARN_UNUSED_RESULT;

  // Load the certificate authority (PEM encoded).
  Status LoadCertificateAuthority(const std::string& certificate_path) WARN_UNUSED_RESULT;

  // Initiates a new TlsHandshake instance.
  Status InitiateHandshake(TlsHandshake* handshake) const WARN_UNUSED_RESULT;

  // Return the number of certs that have been marked as trusted.
  // Used by tests.
  int trusted_cert_count_for_tests() const {
    shared_lock<RWMutex> lock(lock_);
    return trusted_cert_count_;
  }

  bool is_external_cert() const { return is_external_cert_; }

 private:

  Status VerifyCertChainUnlocked(const Cert& cert) WARN_UNUSED_RESULT;

  // The cipher suite preferences to use for RPC connections secured with
  // pre-TLSv1.3 protocols. Uses the OpenSSL cipher preference list format.
  // See man (1) ciphers for more information.
  std::string tls_ciphers_;

  // TLSv1.3-specific ciphersuites. These are controlled separately from the
  // pre-TLSv1.3 ones because the OpenSSL API provides separate calls to set
  // those and the syntax for the TLSv1.3 list differs from the syntax for
  // the legacy pre-TLSv1.3 ones. See man (1) ciphers for more information.
  std::string tls_ciphersuites_;

  // The minimum protocol version to allow for securing RPC connections with
  // TLS. May be one of 'TLSv1', 'TLSv1.1', 'TLSv1.2', 'TLSv1.3'.
  std::string tls_min_protocol_;

  // TLS protocol versions to exclude from the list of acceptable ones to secure
  // RPC connections with TLS. An empty container means the set of acceptable
  // protocol version is defined by 'tls_min_protocol_' and the OpenSSL library
  // itself.
  std::vector<std::string> tls_excluded_protocols_;

  // Protects all members.
  //
  // Taken in write mode when any changes are modifying the underlying SSL_CTX
  // using a mutating method (eg SSL_CTX_use_*) or when changing the value of
  // any of our own member variables.
  mutable RWMutex lock_;
  c_unique_ptr<SSL_CTX> ctx_;
  int32_t trusted_cert_count_;
  bool has_cert_;
  bool is_external_cert_;
  std::optional<CertSignRequest> csr_;
};

} // namespace security
} // namespace kudu
