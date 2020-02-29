{-# OPTIONS_GHC -funbox-strict-fields -Werror #-}

{-
    TODO Fill out reduce.
    TODO K should not evaluate tail.
      TODO Or, find a way to make K not need to short-circuit.

    Note that On 64 bit machines, GHC will always use pointer tagging
    as long as there are less than 8 constructors. So, anything that is
    frequently pattern matched on should have at most 7 branches.
-}

module Uruk.Fast.Types where

import ClassyPrelude             hiding (evaluate, fromList, try, seq)
import Control.Monad.Primitive
import Data.Primitive.Array
import Data.Primitive.SmallArray
import GHC.Prim                  hiding (seq)
import System.IO.Unsafe

import Control.Arrow     ((>>>))
import Control.Exception (throw, try)
import Data.Bits         (shiftL, (.|.))
import Data.Flat
import Data.Function     ((&))
import Numeric.Natural   (Natural)
import Prelude           ((!!))
import Uruk.JetDemo      (Ur, UrPoly(Fast))

import qualified GHC.Exts
import qualified Urbit.Atom   as Atom
import qualified Uruk.JetDemo as Ur


-- Useful Types ----------------------------------------------------------------

type Nat = Natural
type Bol = Bool


-- Closure ---------------------------------------------------------------------

type CloN = SmallArray Val

getCloN :: CloN -> Int -> Val
{-# INLINE getCloN #-}
getCloN = indexSmallArray

addCloN :: CloN -> Val -> CloN
{-# INLINE addCloN #-}
addCloN xs x = xs <> GHC.Exts.fromList [x] -- TODO Slow

clo1 :: Val -> CloN
clo1 x = GHC.Exts.fromList [x]


-- Arguments -------------------------------------------------------------------

type ArgN = SmallArray (IO Val)


-- Registers -------------------------------------------------------------------

type RegN = SmallMutableArray RealWorld Val

newRegN :: Int -> IO RegN
{-# INLINE newRegN #-}
newRegN n = newSmallArray n VUni

getRegN :: RegN -> Int -> IO Val
{-# INLINE getRegN #-}
getRegN = readSmallArray

setRegN :: RegN -> Int -> Val -> IO ()
{-# INLINE setRegN #-}
setRegN = writeSmallArray

instance Hashable a => Hashable (SmallArray a) where
  hashWithSalt i x = hashWithSalt i (GHC.Exts.toList x)


-- Types -----------------------------------------------------------------------

data Jet = Jet
  { jArgs :: !Int
  , jName :: Val
  , jBody :: Val
  , jFast :: !Exp
  , jRegs :: !Int -- Number of registers needed.
  }
 deriving (Eq, Ord)

instance Hashable Jet where
  hashWithSalt i (Jet a n b _ _) =
    hashWithSalt i (a,n,b)

instance Show Jet where
  show (Jet{..}) =
    case jName of
      VNat (Atom.atomUtf8 -> Right nm) ->
        "J" <> show jArgs <> "_" <> unpack nm <> "_" <> has
      _ ->
        "J" <> show jArgs <> "_" <> show jName <> "_" <> has
   where
     has = (take 5 $ show $ abs $ hash jBody)

data Node
  = Jay Int -- Always >= 1
  | Kay
  | Ess
  | Dee
  | Jut Jet
  | Eye
  | Bee
  | Sea
  | Sen Int --  Always >=  1
  | Ben Int --  Always >=  1
  | Cen Int --  Always >=  1
  | Seq
  | Yet Nat
  | Fix
  | Nat Nat
  | Bol Bool
  | Iff
  | Pak
  | Zer
  | Eql
  | Add
  | Inc
  | Dec
  | Fec
  | Mul
  | Sub
  | Ded
  | Uni
  | Lef
  | Rit
  | Cas
  | Con
  | Car
  | Cdr
 deriving (Eq, Ord, Generic, Hashable)

instance Show Node where
  show = \case
    Jay n     -> replicate (fromIntegral n) 'J'
    Kay       -> "K"
    Ess       -> "S"
    Dee       -> "D"
    Jut j     -> show j
    Eye       -> "I"
    Bee       -> "B"
    Sea       -> "C"
    Sen n     -> "S" <> show n
    Ben n     -> "B" <> show n
    Cen n     -> "C" <> show n
    Seq       -> "SEQ"
    Yet n     -> "YET" <> show n
    Fix       -> "FIX"
    Nat n     -> show n
    Bol True  -> "%.y"
    Bol False -> "%.n"
    Iff       -> "IFF"
    Pak       -> "PAK"
    Zer       -> "ZER"
    Eql       -> "EQL"
    Add       -> "ADD"
    Inc       -> "INC"
    Dec       -> "DEC"
    Fec       -> "FEC"
    Mul       -> "MUL"
    Sub       -> "SUB"
    Ded       -> "DED"
    Uni       -> "UNI"
    Lef       -> "LEF"
    Rit       -> "RIT"
    Cas       -> "CAS"
    Con       -> "CON"
    Car       -> "CAR"
    Cdr       -> "CDR"

data Fun = Fun
  { fNeed :: !Int
  , fHead :: !Node
  , fArgs :: CloN -- Lazy on purpose.
  }
 deriving (Eq, Ord, Generic, Hashable)

instance Show Fun where
  show (Fun _ h args) = if sizeofSmallArray args == 0
    then show h
    else mconcat
      ["(", show h <> " ", intercalate " " (show <$> GHC.Exts.toList args), ")"]

data Val
  = VUni
  | VCon !Val !Val
  | VLef !Val
  | VRit !Val
  | VNat !Nat
  | VBol !Bool
  | VFun !Fun
 deriving (Eq, Ord, Generic, Hashable)

instance Show Val where
  show = \case
    VUni       -> "~"
    VCon x y   -> "[" <> show x <> " " <> show y <> "]"
    VLef x     -> "L" <> show x
    VRit x     -> "R" <> show x
    VNat n     -> show n
    VBol True  -> "%.y"
    VBol False -> "%.n"
    VFun f     -> show f

data Exp
  = VAL   !Val                    --  Constant Value
  | REF   !Int                    --  Stack Reference
  | REG   !Int                    --  Register Reference
  | SLF                           --  Self Reference

  | IFF   !Exp !Exp !Exp          --  If-Then-Else
  | CAS   !Int !Exp !Exp !Exp     --  Pattern Match
  | REC1  !Exp                    --  Recursive Call
  | REC1R !Exp                    --  Recursive Call (No Registers)
  | REC2  !Exp !Exp               --  Recursive Call
  | REC2R !Exp !Exp               --  Recursive Call (No Registers)
  | REC3  !Exp !Exp !Exp          --  Recursive Call
  | REC3R !Exp !Exp !Exp          --  Recursive Call (No Registers)
  | REC4  !Exp !Exp !Exp !Exp     --  Recursive Call
  | REC4R !Exp !Exp !Exp !Exp     --  Recursive Call (No Registers)
  | RECN  !(SmallArray Exp)       --  Recursive Call
  | RECNR !(SmallArray Exp)       --  Recursive Call (No Registers)

  | SEQ !Exp !Exp                 --  Evaluate head, return tail
  | DED !Exp                      --  Evaluate argument, then crash.

  | INC !Exp                      --  Increment
  | DEC !Exp                      --  Decrement
  | FEC !Exp                      --  Fast decrement
  | ADD !Exp !Exp                 --  Add
  | MUL !Exp !Exp                 --  Multiply
  | SUB !Exp !Exp                 --  Subtract
  | ZER !Exp                      --  Is Zero?
  | EQL !Exp !Exp                 --  Atom equality.

  | CON !Exp !Exp                 --  Cons
  | CAR !Exp                      --  Head
  | CDR !Exp                      --  Tail
  | LEF !Exp                      --  Left Constructor
  | RIT !Exp                      --  Right Constructor

  | JET1 !Jet !Exp                --  Fully saturated jet call.
  | JET2 !Jet !Exp !Exp           --  Fully saturated jet call.
  | JET3 !Jet !Exp !Exp !Exp      --  Fully saturated jet call.
  | JET4 !Jet !Exp !Exp !Exp !Exp --  Fully saturated jet call.
  | JETN !Jet !(SmallArray Exp)   --  Fully saturated jet call.

  | CLON !Fun !(SmallArray Exp)   --  Undersaturated call
  | CALN !Exp !(SmallArray Exp)   --  Call of unknown saturation
 deriving (Eq, Ord, Show)


-- Exceptions ------------------------------------------------------------------

data TypeError = TypeError Text
 deriving (Eq, Ord, Show, Exception)

data Crash = Crash Val
 deriving (Eq, Ord, Show, Exception)

data BadRef = BadRef Jet Int
 deriving (Eq, Ord, Show, Exception)


--------------------------------------------------------------------------------

valFun :: Val -> Fun
{-# INLINE valFun #-}
valFun = \case
  VUni     -> Fun 1 Uni mempty
  VCon h t -> Fun 1 Con (fromList [h, t])
  VLef l   -> Fun 2 Lef (fromList [l])
  VRit r   -> Fun 2 Lef (fromList [r])
  VNat n   -> Fun 2 (Nat n) mempty
  VBol b   -> Fun 2 (Bol b) mempty
  VFun f   -> f

nodeArity :: Node -> Int
nodeArity = \case
  Jay _ -> 2
  Kay   -> 2
  Ess   -> 3
  Dee   -> 1
  Jut j -> jArgs j
  Eye   -> 1
  Bee   -> 3
  Sea   -> 3
  Sen n -> 2+n
  Ben n -> 2+n
  Cen n -> 2+n
  Seq   -> 2
  Yet n -> fromIntegral (1+n)
  Fix   -> 2
  Nat n -> 2
  Bol b -> 2
  Iff   -> 3
  Pak   -> 1
  Zer   -> 1
  Eql   -> 2
  Add   -> 2
  Inc   -> 1
  Dec   -> 1
  Fec   -> 1
  Mul   -> 2
  Sub   -> 2
  Ded   -> 1
  Uni   -> 1
  Lef   -> 1
  Rit   -> 1
  Cas   -> 4
  Con   -> 3
  Car   -> 1
  Cdr   -> 1
