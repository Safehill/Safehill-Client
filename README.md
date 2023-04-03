# Safehill-Crypto

A (soon-to-be-cross-platform) library to manage encryption and decryption of data.


## Sharing encrypted content


All users have a set of locally generated asymmetric keys (private P, public Q):
- Alice has private key PA and corresponding public key QA
- Bob has private key PB and corresponding public key QB
- Charlie has private key PC and corresponding public key QC

Their public key is usually shared publicly with everyone, on a server.



### Encrypting and sharing with a subset of users
_Alice (A) wants to share d1 securely and privately with Bob (B) and Charlie (C)._

In order to do so, Alice does the following:
1. Alice encrypts the unencrypted data d1 (CLEAR) using a symmetric key Pd1 to obtain Ed1 (CYPHER)
        Ed1 = E(d1, Pd1)
2. Alice stores Pd1 in the keychain (protecting data at rest). The keychain is usually synced, so Alice can get to the data from any of her devices.
3. Alice then encrypts Pd1 with Bob and Charlie's public keys (QB and QC, respectively) to get EBPd1 and ECPd1 (encrypted Pd1 for Bob and Charlie, respectively)
        EBPd1 = E(Pd1, QB)
        ECPd1 = E(Pd1, QC)
4. Alice sends Ed1 and EBPd1 to Bob, and Ed1 and ECPd1 to Charlie
5. Bob decrypts EBPd1 using his private key PB to obtain d1's decryption key, then decrypts Ed1
        Pd1 = D(EBPd1, PB)
        d1 = D(Ed1, Pd1)
6. Charlie decrypts ECPd1 using his private key PB to obtain d1's decryption key, then decrypts Ed1
        Pd1 = D(ECPd1, PC)
        d1 = D(Ed1, Pd1)
7. All 3 parties have now access to d1, but noone else can read the data


        
### Adding users to the share
_Alice now wants to add Dave (D) to the existing share of d1._

This process only requires to share extra encrypted key with Dave (or uploading extra metadata to a server).
It does not involve other users in the share and does not require any modifications to the content.

1. Alice encrypts Pd1 with Dave's public key (QD) to get EDPd1 (encrypted Pd1 for Dave)
        EDPd1 = E(Pd1, QD)
2. Alice sends Ed1 and EDPd1 to Dave
3. Dave - as Bob and Charlie - decrypts EDPd1 using his private key PD to obtain d1's decryption key, then decrypts Ed1
        Pd1 = D(EDPd1, PD)
        d1 = D(Ed1, Pd1)



### Revoking access
_Alice now wants to revoke Charlie's access to d1_

1. Alice re-encrypts d1 with a new symmetric key P'd1
        E'd1 = E(d1, P'd1)
2. Alice encrypts P'd1 with Bob's public key QB
        EBP'd1 = E(P'd1, QB)
3. Alice notifies Bob about the change, by sending him both E'd1 and EBP'd1 (or uploading it to a server).
